//! End-to-end read-fulfillment coverage over the real HTTP dispatcher.
//!
//! These checks deliberately warm shared decoded payload caches before
//! changing the authority. The request still carries the older descriptor
//! roots, so a successful read proves that discovery stays pinned to those
//! roots while exercising the operation-sized blob closure and continuation
//! protocol.
use super::file_open_probe::{TimedClient, authority_execute};
use super::*;
use crate::sync::SyncTransport;
use crate::sync::http::{RawHttpClient, RawHttpRequest, RawHttpResponse};
use std::collections::BTreeSet;

#[derive(Clone)]
struct ReadOnlyProductionClient {
    client: reqwest::Client,
    repo_path: String,
}

#[derive(Clone)]
struct CaptureBlobInputsClient {
    inner: Client,
    manifests: Arc<std::sync::Mutex<BTreeSet<String>>>,
    chunks: Arc<std::sync::Mutex<BTreeSet<String>>>,
}

impl RawHttpClient for CaptureBlobInputsClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        let fulfillment = request.url.ends_with("/sync/read-fulfillment");
        Box::pin(async move {
            let response = self.inner.send(request).await?;
            if fulfillment && (200..300).contains(&response.status) {
                let response: crate::sync::read_fulfillment::ReadFulfillmentResponse =
                    serde_json::from_slice(&response.body).map_err(|_| {
                        LixError::new(
                            "TEST_INVALID_READ_FULFILLMENT",
                            "authority returned an invalid read fulfillment response",
                        )
                    })?;
                for input in response.inputs {
                    match input.address {
                        crate::sync::read_fulfillment::ReadInputAddress::BlobManifest(hash) => {
                            self.manifests
                                .lock()
                                .expect("captured manifest lock")
                                .insert(crate::binary_cas::BlobId::from_bytes(hash).to_hex());
                        }
                        crate::sync::read_fulfillment::ReadInputAddress::BlobChunk(hash) => {
                            self.chunks
                                .lock()
                                .expect("captured chunk lock")
                                .insert(crate::binary_cas::ChunkHash::from_bytes(hash).to_hex());
                        }
                        _ => {}
                    }
                }
            }
            Ok(response)
        })
    }
}

impl RawHttpClient for ReadOnlyProductionClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            let parsed = url::Url::parse(&request.url)
                .map_err(|_| LixError::new("LIX_PRODUCTION_PROBE_INVALID_URL", "invalid URL"))?;
            let path = parsed.path();
            let repo_route = path
                .strip_prefix(&self.repo_path)
                .filter(|suffix| suffix.is_empty() || suffix.starts_with('/'));
            let allowed = repo_route.is_some_and(|route| match request.method.as_str() {
                "GET" => route.is_empty() || route == "/" || route == "/sync/descriptor",
                "POST" => [
                    "/sync/read-fulfillment",
                    "/sync/native-objects",
                    "/sync/native-object-range",
                    "/sync/native-metadata",
                    "/sync/native-metadata-walk",
                ]
                .contains(&route),
                "DELETE" => route == "/session",
                _ => false,
            });
            if !allowed
                || parsed.scheme() != "http"
                || parsed.host_str() != Some("127.0.0.1")
                || parsed.port() != Some(43019)
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.query().is_some()
                || parsed.fragment().is_some()
            {
                return Err(LixError::new(
                    "LIX_PRODUCTION_PROBE_ROUTE_BLOCKED",
                    "production probe client permits only the local read-only relay routes",
                ));
            }
            let mut builder = self.client.request(request.method.clone(), &request.url);
            for (name, value) in request.headers {
                builder = builder.header(name, value);
            }
            if let Some(body) = request.body {
                builder = builder.body(body);
            }
            let mut response = builder.send().await.map_err(|_| {
                LixError::new(
                    "LIX_PRODUCTION_PROBE_TRANSPORT",
                    "production probe request failed",
                )
            })?;
            let status = response.status();
            if status.is_redirection() {
                return Err(LixError::new(
                    "LIX_PRODUCTION_PROBE_REDIRECT_BLOCKED",
                    "production probe relay redirects are forbidden",
                ));
            }
            if response
                .content_length()
                .is_some_and(|length| length > request.response_limit as u64)
            {
                return Err(LixError::new(
                    "LIX_PRODUCTION_PROBE_RESPONSE_TOO_LARGE",
                    "production probe response exceeds its route bound",
                ));
            }
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| {
                LixError::new(
                    "LIX_PRODUCTION_PROBE_TRANSPORT",
                    "production probe response stream failed",
                )
            })? {
                if body.len().saturating_add(chunk.len()) > request.response_limit {
                    return Err(LixError::new(
                        "LIX_PRODUCTION_PROBE_RESPONSE_TOO_LARGE",
                        "production probe response exceeds its route bound",
                    ));
                }
                body.extend_from_slice(&chunk);
            }
            Ok(RawHttpResponse {
                status: status.as_u16(),
                status_text: status.to_string(),
                body,
            })
        })
    }
}

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

async fn observe_first<S>(
    lix: &Lix<S>,
    sql: &str,
    params: &[Value],
) -> Result<ExecuteResult, LixError>
where
    S: crate::storage_adapter::Storage + Clone + Send + Sync + 'static,
{
    let mut events = lix.observe(sql, params)?;
    let event = tokio::time::timeout(std::time::Duration::from_secs(20), events.next())
        .await
        .map_err(|_| {
            LixError::new(
                "LIX_TEST_OBSERVE_TIMEOUT",
                "observe did not yield its first result",
            )
        })??
        .ok_or_else(|| {
            LixError::new(
                "LIX_TEST_OBSERVE_ENDED",
                "observe ended before its first result",
            )
        })?;
    events.close();
    Ok(event.rows)
}

#[tokio::test]
async fn cold_global_session_and_ranged_reads_use_read_fulfillment() {
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

    let selected_branch_id = replica
        .active_branch_id()
        .await
        .expect("selected branch should be available after partial open");
    log.lock().unwrap().clear();
    let global_session = replica
        .open_another_session()
        .with_branch(crate::GLOBAL_BRANCH_ID)
        .await
        .expect("partial replica should open an independent global-branch session");
    let default_branch = global_session
        .execute(
            "SELECT value FROM lix_key_value WHERE key = $1 AND lixcol_file_id IS NULL AND lixcol_untracked = false",
            &[Value::Text(crate::init::DEFAULT_BRANCH_KEY.to_owned())],
        )
        .await
        .expect("global session should hydrate the repository default branch");
    assert_eq!(default_branch.rows().len(), 1);
    let default_branch_id = match default_branch.rows()[0]
        .get::<Value>("value")
        .expect("default branch id should decode")
    {
        Value::Jsonb(value) => value
            .as_json_string()
            .expect("default branch JSONB should contain a string"),
        Value::Text(value) => value,
        other => panic!("unexpected default branch value: {other:?}"),
    };
    assert_eq!(
        default_branch_id,
        authority.active_branch_id().await.unwrap()
    );
    assert!(
        !fulfillment_requests(&log).is_empty(),
        "cold global-branch metadata read must use read fulfillment"
    );
    global_session
        .close()
        .await
        .expect("global-branch session should close");
    assert_eq!(
        replica
            .active_branch_id()
            .await
            .expect("selected branch should remain available"),
        selected_branch_id,
        "opening and closing a global session must preserve the selected branch",
    );

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

#[tokio::test]
async fn cold_sparse_checkpoint_conversations_hydrate_through_target_in_scan() {
    const CONVERSATION_IDS: [&str; 4] = [
        "01950000-0000-7000-8000-000000001001",
        "01950000-0000-7000-8000-000000001003",
        "01950000-0000-7000-8000-000000001004",
        "01950000-0000-7000-8000-000000001006",
    ];
    const OUTSIDE_CONVERSATION_ID: &str = "01950000-0000-7000-8000-000000001002";
    const LOCAL_CONVERSATION_ID: &str = "01950000-0000-7000-8000-000000001005";

    let print_profile = |case: &str, log: &Arc<std::sync::Mutex<Vec<serde_json::Value>>>| {
        let requests = fulfillment_requests(log);
        let rpc_measurements = requests
            .iter()
            .map(|request| {
                serde_json::json!({
                    "inputs": request["inputs"],
                    "response_bytes": request["response_bytes"],
                    "discovery": request["discovery"],
                    "server_ms": request["server_ms"],
                })
            })
            .collect::<Vec<_>>();
        let scan_limits = requests
            .iter()
            .flat_map(|request| {
                request["request_interests"]
                    .as_array()
                    .into_iter()
                    .flatten()
            })
            .filter(|interest| {
                interest["kind"] == "scan"
                    && interest["request"]["filter"]["schema_keys"]
                        .as_array()
                        .is_some_and(|keys| keys.iter().any(|key| key == "lix_conversation"))
            })
            .filter_map(|interest| interest["request"]["limit"].as_u64())
            .filter(|limit| *limit > 0)
            .collect::<Vec<_>>();
        eprintln!(
            "CONVERSATION_READ_FULFILLMENT_PROFILE_JSON={}",
            serde_json::json!({
                "case": case,
                "rpcs": rpc_measurements,
                "scan_limits": scan_limits,
            })
        );
    };

    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();

    let mut checkpoint_ids = Vec::new();
    for index in 0..4 {
        authority
            .execute(
                "INSERT INTO lix_file(path, content) VALUES ($1, $2)",
                &[
                    Value::Text(format!("/conversation-checkpoint-{index}.txt")),
                    Value::Blob(format!("checkpoint {index}").into_bytes().into()),
                ],
            )
            .await
            .unwrap();
        let checkpoint_id = authority
            .execute(
                "SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        authority
            .execute(
                "INSERT INTO lix_conversation (id, target, title, lixcol_global) \
                 VALUES ($1, lix_row_ref('lix_commit', NULL, $2), $3, true)",
                &[
                    Value::Text(CONVERSATION_IDS[index].to_owned()),
                    Value::Text(checkpoint_id.clone()),
                    Value::Text(format!("Qualification {index}")),
                ],
            )
            .await
            .unwrap();
        checkpoint_ids.push(checkpoint_id);
    }

    // The target predicate should select only the first four rows. A fifth
    // global conversation and a local file conversation are
    // useful decoys: authority candidate scans may be broader, but SQL's
    // residual predicates must still define the result.
    authority
        .execute(
            "INSERT INTO lix_file(path, content) VALUES ('/conversation-outside-range.txt', CAST('outside' AS BYTEA))",
            &[],
        )
        .await
        .unwrap();
    let outside_checkpoint = authority
        .execute(
            "SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("commit_id")
        .unwrap();
    let local_file_id = authority
        .execute(
            "SELECT id FROM lix_file WHERE path = '/conversation-checkpoint-0.txt'",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();
    authority
        .execute(
            "INSERT INTO lix_conversation (id, target, title, lixcol_global) \
             VALUES ($1, lix_row_ref('lix_commit', NULL, $2), 'Outside qualification', true), \
                    ($3, lix_row_ref('lix_file', NULL, $4), 'Local decoy', false)",
            &[
                Value::Text(OUTSIDE_CONVERSATION_ID.to_owned()),
                Value::Text(outside_checkpoint),
                Value::Text(LOCAL_CONVERSATION_ID.to_owned()),
                Value::Text(local_file_id),
            ],
        )
        .await
        .unwrap();

    let query = "SELECT id, title, \
                    CASE \
                      WHEN target = lix_row_ref('lix_commit', NULL, $1) THEN $1 \
                      WHEN target = lix_row_ref('lix_commit', NULL, $2) THEN $2 \
                      WHEN target = lix_row_ref('lix_commit', NULL, $3) THEN $3 \
                      WHEN target = lix_row_ref('lix_commit', NULL, $4) THEN $4 \
                    END AS commit_id \
                 FROM lix_conversation \
                 WHERE lixcol_global = true \
                   AND target IN ( \
                     lix_row_ref('lix_commit', NULL, $1), \
                     lix_row_ref('lix_commit', NULL, $2), \
                     lix_row_ref('lix_commit', NULL, $3), \
                     lix_row_ref('lix_commit', NULL, $4) \
                   ) \
                 ORDER BY lixcol_created_at ASC, id ASC";
    let params = checkpoint_ids
        .iter()
        .cloned()
        .map(Value::Text)
        .collect::<Vec<_>>();
    let authority_rows = authority.execute(query, &params).await.unwrap();
    assert_eq!(authority_rows.rows().len(), 4);
    let expected = checkpoint_ids
        .iter()
        .enumerate()
        .map(|(index, commit_id)| (commit_id.clone(), format!("Qualification {index}")))
        .collect::<std::collections::BTreeMap<_, _>>();
    let authority_results = authority_rows
        .rows()
        .iter()
        .map(|row| {
            (
                row.get::<String>("commit_id").unwrap(),
                row.get::<String>("title").unwrap(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(authority_results, expected);

    // An ordered declared column exercises range candidates without assigning
    // ordering semantics to opaque ROW_REF values.
    let range_schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "candidate_range_probe",
        "columns": [
            {"name": "id", "type": "text", "nullable": false},
            {"name": "ordinal", "type": "int8", "nullable": false}
        ],
        "primary_key": ["id"],
        "unique": [["ordinal"]]
    });
    authority
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(serde_json::to_string(&range_schema).unwrap())],
        )
        .await
        .unwrap();
    authority
        .execute(
            "INSERT INTO candidate_range_probe (id, ordinal) VALUES \
             ('below', -1), ('first', 0), ('middle', 1), ('last', 2), ('above', 3)",
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
    let open_cold_replica = |log: Arc<std::sync::Mutex<Vec<serde_json::Value>>>| {
        let server = server.clone();
        let repository_id = authority.lix_id().to_owned();
        let account_id = authority.active_account_id().to_owned();
        async move {
            let transport = HttpSyncTransport::connect_with(
                TimedClient {
                    inner: Client {
                        server,
                        lose_body: Arc::new(AtomicBool::new(false)),
                    },
                    log,
                    delay: 0,
                },
                &format!("https://example.test/lix/{repository_id}"),
            )
            .await
            .unwrap();
            let leased = transport.partial_replica_descriptor(None).await.unwrap();
            let state = Arc::new(
                PartialReplicaState::from_leased(
                    transport.protocol_url().into(),
                    account_id,
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
            let worker_storage = storage;
            let worker_state = state;
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
            let replica = Lix::from_partial_engine_for_test(Arc::clone(&engine), session, sender);
            (replica, worker)
        }
    };

    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (replica, worker) = open_cold_replica(log.clone()).await;

    let rows = replica
        .execute(query, &params)
        .await
        .expect("cold sparse target-IN query should hydrate its global conversations");
    let results = rows
        .rows()
        .iter()
        .map(|row| {
            (
                row.get::<String>("commit_id").unwrap(),
                row.get::<String>("title").unwrap(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(results, expected);
    assert!(
        !fulfillment_requests(&log).is_empty(),
        "checkpoint conversation query must use read fulfillment"
    );
    let requests = fulfillment_requests(&log);
    let conversation_scan = requests
        .iter()
        .flat_map(|request| {
            request["request_interests"]
                .as_array()
                .into_iter()
                .flatten()
        })
        .find(|interest| {
            interest["kind"] == "scan"
                && interest["request"]["filter"]["schema_keys"]
                    .as_array()
                    .is_some_and(|keys| keys.iter().any(|key| key == "lix_conversation"))
        })
        .expect("cold query should select the conversation scan recipe");
    let filter = &conversation_scan["request"]["filter"];
    assert_eq!(filter["row_pks"].as_array().map(Vec::len), Some(0));
    assert_eq!(
        filter["declared_column_eq"]["values"]
            .as_array()
            .map(Vec::len),
        Some(4),
        "the cold scan should carry all four target-IN values"
    );
    print_profile("target_in", &log);

    replica.close().await.unwrap();
    worker.abort();

    // Re-run the same bounded predicate on another empty replica with a
    // positive SQL limit. It must remain a cold read and return the full
    // target set before LIMIT is applied locally.
    let limited_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (limited_replica, limited_worker) = open_cold_replica(limited_log.clone()).await;
    let limited_query = format!("{query} LIMIT 4");
    let limited_rows = limited_replica
        .execute(&limited_query, &params)
        .await
        .expect("cold target-IN query with LIMIT should hydrate its candidates");
    let limited_results = limited_rows
        .rows()
        .iter()
        .map(|row| {
            (
                row.get::<String>("commit_id").unwrap(),
                row.get::<String>("title").unwrap(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(limited_results, expected);
    assert!(!fulfillment_requests(&limited_log).is_empty());
    print_profile("limited", &limited_log);
    limited_replica.close().await.unwrap();
    limited_worker.abort();

    // Exercise a genuine declared-column range on another empty replica.
    let range_query = "SELECT id FROM candidate_range_probe \
                       WHERE ordinal BETWEEN 0 AND 2 ORDER BY id";
    let authority_range = authority.execute(range_query, &[]).await.unwrap();
    let expected_range = authority_range
        .rows()
        .iter()
        .map(|row| row.get::<String>("id").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(expected_range, ["first", "last", "middle"]);
    let range_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (range_replica, range_worker) = open_cold_replica(range_log.clone()).await;
    let range_rows = range_replica
        .execute(range_query, &[])
        .await
        .expect("cold indexed integer range should hydrate its candidates");
    let range_results = range_rows
        .rows()
        .iter()
        .map(|row| row.get::<String>("id").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(range_results, expected_range);
    assert!(!fulfillment_requests(&range_log).is_empty());
    let requests = fulfillment_requests(&range_log);
    assert!(requests.iter().any(|request| {
        request["request_interests"]
            .as_array()
            .is_some_and(|interests| {
                interests.iter().any(|interest| {
                    interest["kind"] == "scan"
                        && interest["request"]["filter"]["schema_keys"]
                            .as_array()
                            .is_some_and(|keys| {
                                keys.iter().any(|key| key == "candidate_range_probe")
                            })
                        && interest["request"]["filter"]["declared_column_range"].is_object()
                })
            })
    }));
    print_profile("range", &range_log);
    range_replica.close().await.unwrap();
    range_worker.abort();

    server.close().await.unwrap();
    authority.close().await.unwrap();
}
/// Manual read-only probe for reproducing production-only partial-replica
/// failures against an authority through the real HTTP transport. The local
/// replica is always a fresh in-memory store. Set
/// `LIX_READ_FULFILLMENT_PROBE_URL` to a normalized repository URL when
/// running this ignored test; it performs descriptor/fulfillment reads and
/// closes its HTTP session, with no authority writes beyond the baseline
/// lease acquired by the descriptor endpoint.
#[tokio::test]
#[ignore = "manual read-only production protocol probe"]
async fn production_cold_global_sql_uses_read_fulfillment() {
    let url = std::env::var("LIX_READ_FULFILLMENT_PROBE_URL")
        .expect("set LIX_READ_FULFILLMENT_PROBE_URL to the normalized repo URL");
    let normalized = crate::sync::normalize_sync_locator(&url)
        .expect("probe URL should be a canonical loopback repository locator");
    let normalized_url =
        url::Url::parse(&normalized.protocol_url).expect("normalized probe URL should parse");
    assert_eq!(normalized_url.scheme(), "http");
    assert_eq!(normalized_url.host_str(), Some("127.0.0.1"));
    assert_eq!(normalized_url.port(), Some(43019));
    let client = ReadOnlyProductionClient {
        client: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
        repo_path: normalized_url.path().trim_end_matches('/').to_owned(),
    };
    let transport = HttpSyncTransport::connect_with(client, &url)
        .await
        .expect("production sync handshake should succeed");
    let probe_result = run_production_cold_global_probe(&transport).await;
    if let Err(error) = &probe_result {
        report_production_probe_error("probe", error);
    }
    let close_result = transport.close_session().await;
    if let Err(error) = &close_result {
        report_production_probe_error("http_close", error);
    }
    assert!(probe_result.is_ok(), "cold production GLOBAL SQL failed");
    assert!(
        close_result.is_ok(),
        "production HTTP session cleanup failed"
    );
}

async fn run_production_cold_global_probe(
    transport: &HttpSyncTransport<ReadOnlyProductionClient>,
) -> Result<(), LixError> {
    let leased = transport.partial_replica_descriptor(None).await?;
    let state = Arc::new(PartialReplicaState::from_leased(
        transport.protocol_url().into(),
        transport.active_account_id().into(),
        uuid::Uuid::now_v7().to_string(),
        leased.wire,
    )?);
    transport.bind_native_baseline_lease(state.baseline_lease())?;

    let storage =
        StorageAdapter::new(crate::storage_adapter::StorageSession::acquire(Memory::new()).await?);
    let read = storage.begin_read(Default::default()).await?;
    let mut writes = storage.new_write_set();
    let preconditions = stage_partial_bootstrap(&read, &mut writes, &state)?;
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
        .await?;
    let (engine, session) =
        Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &state).await?;
    let engine = Arc::new(engine);
    engine.sync_mode().admit_partial_replica(
        state.clone(),
        crate::sync::partial_replica_write_capability(),
    );
    storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());

    let (sender, mut receiver) = tokio::sync::mpsc::channel::<crate::sync::SyncDemand>(16);
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
    let replica = Lix::from_partial_engine_for_test(Arc::clone(&engine), session, sender);

    let result = async {
        let global = replica
            .open_another_session()
            .with_branch(crate::GLOBAL_BRANCH_ID)
            .await?;
        let mut first_error = None;
        let default_branch = global
            .execute(
                "SELECT value FROM lix_key_value WHERE key = $1 AND lixcol_file_id IS NULL AND lixcol_untracked = false",
                &[Value::Text(crate::init::DEFAULT_BRANCH_KEY.to_owned())],
            )
            .await;
        match default_branch {
            Ok(rows) if rows.rows().len() == 1 => {
                eprintln!("production_probe default_branch=ok rows=1");
            }
            Ok(rows) => {
                eprintln!(
                    "production_probe default_branch=unexpected_rows count={}",
                    rows.rows().len()
                );
                first_error = Some(LixError::new(
                    "LIX_PRODUCTION_PROBE_DEFAULT_BRANCH_CARDINALITY",
                    "production default-branch query returned an unexpected row count",
                ));
            }
            Err(error) => {
                report_production_probe_error("default_branch", &error);
                first_error = Some(error);
            }
        }
        let account_rows = global
            .execute(
                "SELECT id FROM lix_account WHERE lixcol_untracked = false LIMIT 10",
                &[],
            )
            .await;
        match account_rows {
            Ok(rows) => eprintln!("production_probe global_accounts=ok rows={}", rows.rows().len()),
            Err(error) => {
                report_production_probe_error("global_accounts", &error);
                first_error.get_or_insert(error);
            }
        }
        if let Err(error) = global.close().await {
            report_production_probe_error("global_close", &error);
            first_error.get_or_insert(error);
        }
        first_error.map_or(Ok(()), Err)
    }
    .await;
    let replica_close = replica.close().await;
    worker.abort();
    result?;
    replica_close?;
    Ok(())
}

fn report_production_probe_error(operation: &str, error: &LixError) {
    let details = error.details.as_deref();
    let enum_field = |key: &str| {
        details
            .and_then(|value| value.get(key))
            .and_then(serde_json::Value::as_str)
            .filter(|value| {
                matches!(
                    *value,
                    "absent"
                        | "matched"
                        | "identity_mismatch"
                        | "missing_snapshot"
                        | "lifetime_mismatch"
                )
            })
    };
    let bool_field = |key: &str| {
        details
            .and_then(|value| value.get(key))
            .and_then(serde_json::Value::as_bool)
    };
    let uuid_field = |key: &str| {
        details
            .and_then(|value| value.get(key))
            .and_then(serde_json::Value::as_str)
            .filter(|value| uuid::Uuid::parse_str(value).is_ok())
    };
    eprintln!(
        "production_probe {operation}=error code={} standalone={:?} physical={:?} deferred={:?} conflict={:?} change_id={:?} source_commit_id={:?}",
        error.code,
        enum_field("payloadStandaloneStatus"),
        enum_field("payloadPhysicalStatus"),
        bool_field("payloadSourceDeferred"),
        bool_field("payloadPhysicalConflict"),
        uuid_field("changeId"),
        uuid_field("sourceCommitId"),
    );
}

#[tokio::test]
async fn bounded_preview_and_native_diagnostic_queries_work_over_partial_http() {
    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    let large_id = "0193182b-2a72-7ed5-9015-76bf271af336";
    let small_id = "0193182b-2a72-7ed5-9015-76bf271af337";
    let large_bytes = vec![0x19; 300 * 1024];
    let small_bytes = vec![0x5a; 16 * 1024];
    authority
        .execute(
            "INSERT INTO lix_file (id, path, content) VALUES ($1, '/preview-large', $2), ($3, '/preview-small', $4)",
            &[
                Value::Text(large_id.into()),
                Value::Blob(large_bytes.clone().into()),
                Value::Text(small_id.into()),
                Value::Blob(small_bytes.clone().into()),
            ],
        )
        .await
        .unwrap();
    let large_blob_id = crate::binary_cas::BlobId::from_content(&large_bytes).to_hex();
    let small_blob_id = crate::binary_cas::BlobId::from_content(&small_bytes).to_hex();

    let server = open_lix()
        .with_storage(backing)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    let manifests = Arc::new(std::sync::Mutex::new(BTreeSet::new()));
    let chunks = Arc::new(std::sync::Mutex::new(BTreeSet::new()));
    let transport = HttpSyncTransport::connect_with(
        CaptureBlobInputsClient {
            inner: Client {
                server,
                lose_body: Arc::new(AtomicBool::new(false)),
            },
            manifests: Arc::clone(&manifests),
            chunks: Arc::clone(&chunks),
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

    let registered_schemas = observe_first(
        &replica,
        "SELECT schema_key, value, lixcol_file_id, lixcol_metadata, \
             lixcol_created_at, lixcol_updated_at, lixcol_global, lixcol_change_id, \
             lixcol_author_id, lixcol_commit_id, lixcol_untracked \
             FROM lix_registered_schema LIMIT $1 OFFSET $2",
        &[Value::Integer(10), Value::Integer(0)],
    )
    .await
    .expect("projected registered-schema read should hydrate over HTTP");
    assert!(!registered_schemas.is_empty());
    let working_diff = observe_first(
        &replica,
        "SELECT count(*) AS file_count FROM lix_diff('lix_file')",
        &[],
    )
    .await
    .expect("working-diff count should resolve its native read dependencies");
    assert_eq!(working_diff.len(), 1);
    let active_account = observe_first(
        &replica,
        "SELECT id, name FROM lix_account WHERE id = lix_active_account_id()",
        &[],
    )
    .await
    .expect("active-account read should hydrate over HTTP");
    assert_eq!(active_account.len(), 1);

    let preview = observe_first(
        &replica,
        "SELECT id, path, CASE WHEN OCTET_LENGTH(content) <= $1 THEN content END AS content, \
             OCTET_LENGTH(content) AS size_bytes FROM lix_file \
             WHERE id IN ($2, $3) ORDER BY path",
        &[
            Value::Integer(32 * 1024),
            Value::Text(large_id.into()),
            Value::Text(small_id.into()),
        ],
    )
    .await
    .expect("bounded preview should fetch only eligible content over HTTP");
    assert_eq!(preview.len(), 2);
    let large = preview
        .rows()
        .iter()
        .find(|row| row.get::<String>("id").unwrap() == large_id)
        .expect("large file row");
    assert_eq!(large.get::<Value>("content").unwrap(), Value::Null);
    assert_eq!(
        large.get::<i64>("size_bytes").unwrap(),
        i64::try_from(large_bytes.len()).unwrap()
    );
    let small = preview
        .rows()
        .iter()
        .find(|row| row.get::<String>("id").unwrap() == small_id)
        .expect("small file row");
    assert_eq!(small.get::<Vec<u8>>("content").unwrap(), small_bytes);
    assert_eq!(
        small.get::<i64>("size_bytes").unwrap(),
        i64::try_from(small_bytes.len()).unwrap()
    );
    assert_eq!(*manifests.lock().unwrap(), BTreeSet::from([small_blob_id]));
    let expected_chunks = crate::binary_cas::CanonicalBlobManifest::from_bytes(&small_bytes)
        .chunks
        .into_iter()
        .map(|chunk| chunk.hash.to_hex())
        .collect::<BTreeSet<_>>();
    assert_eq!(*chunks.lock().unwrap(), expected_chunks);
    assert!(!manifests.lock().unwrap().contains(&large_blob_id));

    replica.close().await.unwrap();
    worker.abort();
    authority.close().await.unwrap();
}

#[tokio::test]
async fn bounded_checkpoint_file_history_discovers_native_closure() {
    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    let mut checkpoint_ids = Vec::new();
    for checkpoint in 0..12 {
        // Several commits between checkpoints expose multiple dependency layers.
        for edit in 0..3 {
            authority
                .execute(
                    "INSERT INTO lix_file(path, content) VALUES ($1, $2)",
                    &[
                        Value::Text(format!(
                            "/bounded-history/checkpoint-{checkpoint}/{edit}.txt"
                        )),
                        Value::Blob(
                            format!("checkpoint {checkpoint} edit {edit}")
                                .into_bytes()
                                .into(),
                        ),
                    ],
                )
                .await
                .unwrap();
        }
        checkpoint_ids.push(authority.create_checkpoint().await.unwrap().commit_id);
    }
    let mixed_conversation_id = uuid::Uuid::now_v7().to_string();
    authority
        .execute(
            "INSERT INTO lix_conversation(id, target, title, lixcol_global) VALUES ($1, lix_row_ref('lix_commit', NULL, $2), 'Mixed History recovery', true)",
            &[
                Value::Text(mixed_conversation_id.clone()),
                Value::Text(checkpoint_ids.last().unwrap().clone()),
            ],
        )
        .await
        .unwrap();
    let mixed_conversation_change_id = authority
        .execute(
            "SELECT lixcol_change_id FROM lix_conversation WHERE id = $1",
            &[Value::Text(mixed_conversation_id.clone())],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("lixcol_change_id")
        .unwrap();
    let selected = checkpoint_ids
        .iter()
        .rev()
        .take(10)
        .cloned()
        .collect::<Vec<_>>();
    let placeholders = (2..=11)
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT lixcol_to_commit_id AS commit_id, id, coalesce(to_path, from_path) AS path \
                       FROM lix_history('lix_file', $1) \
                       WHERE lixcol_to_commit_id IN ({placeholders}) ORDER BY path ASC"
    );
    let params = std::iter::once(Value::Text(selected[0].clone()))
        .chain(selected.iter().cloned().map(Value::Text))
        .collect::<Vec<_>>();
    let expected = authority.execute(&sql, &params).await.unwrap();
    let values = |result: &ExecuteResult| {
        result
            .rows()
            .iter()
            .map(|row| {
                (
                    row.get::<String>("commit_id").unwrap(),
                    row.get::<String>("id").unwrap(),
                    row.get::<String>("path").unwrap(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        expected.rows().len(),
        30,
        "ten real checkpoint diffs each contain three file writes"
    );
    assert!(
        expected
            .rows()
            .iter()
            .all(|row| selected.contains(&row.get::<String>("commit_id").unwrap()))
    );
    let server = open_lix()
        .with_storage(backing)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    let open_cold_replica_with_delay = |log: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
                                        delay| {
        let server = server.clone();
        let repository_id = authority.lix_id().to_owned();
        let account_id = authority.active_account_id().to_owned();
        async move {
            let transport = HttpSyncTransport::connect_with(
                TimedClient {
                    inner: Client {
                        server,
                        lose_body: Arc::new(AtomicBool::new(false)),
                    },
                    log,
                    delay,
                },
                &format!("https://example.test/lix/{repository_id}"),
            )
            .await
            .unwrap();
            let leased = transport.partial_replica_descriptor(None).await.unwrap();
            let state = Arc::new(
                PartialReplicaState::from_leased(
                    transport.protocol_url().into(),
                    account_id,
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
            let replica = Lix::from_partial_engine_for_test(Arc::clone(&engine), session, sender);
            (replica, worker, storage, state, transport, engine)
        }
    };
    let open_cold_replica = |log| open_cold_replica_with_delay(log, 0);
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (replica, worker, ..) = open_cold_replica(log.clone()).await;
    log.lock().unwrap().clear();
    let started = Instant::now();
    let cold = replica.execute(&sql, &params).await.unwrap();
    assert_eq!(values(&cold), values(&expected));
    let requests = log.lock().unwrap().clone();
    let physical = requests
        .iter()
        .filter(|r| {
            matches!(
                r["operation"].as_str(),
                Some(
                    "native-objects"
                        | "native-object-range"
                        | "native-metadata"
                        | "native-metadata-walk"
                )
            )
        })
        .count();
    let recipes = fulfillment_requests(&log);
    eprintln!(
        "BOUNDED_HISTORY_CLOSURE_PROFILE_JSON={}",
        serde_json::json!({
            "selected_checkpoints": selected.len(), "rows": cold.rows().len(),
            "cold_ms": started.elapsed().as_secs_f64() * 1000.0,
            "physical_fallback_calls": physical, "requests": requests,
            "fulfillment_calls": recipes.len(), "fulfillment_response_bytes": response_bytes(&recipes),
        })
    );
    log.lock().unwrap().clear();
    let warm = replica.execute(&sql, &params).await.unwrap();
    assert_eq!(values(&warm), values(&expected));
    assert!(
        log.lock().unwrap().is_empty(),
        "retained immutable history must execute offline"
    );
    replica.close().await.unwrap();
    worker.abort();
    assert_eq!(
        recipes.len(),
        1,
        "one operation-sized closure must cover the selected page"
    );
    assert!(
        response_bytes(&recipes) < 2 * 1024 * 1024,
        "small history page must retain a bounded wire closure"
    );
    assert!(
        physical <= 2,
        "bounded public checkpoint history needs operation-sized closure; got {physical} pointer fetches"
    );

    // Match the History UI's checkpoint review query: it takes the selected
    // commit's first-parent endpoint from `lix_history` and asks for the
    // relation-scoped fixed Diff. The old native-demand path expands this into
    // one range request per missing commit/mutation object.
    let fixed_to = checkpoint_ids.last().unwrap().clone();
    let parent_rows = authority
        .execute(
            "SELECT lixcol_from_commit_id AS parent_commit_id \
             FROM lix_history('lix_file', $1) \
             WHERE lixcol_to_commit_id = $1 LIMIT 1",
            &[Value::Text(fixed_to.clone())],
        )
        .await
        .unwrap();
    let fixed_from = parent_rows.rows()[0]
        .get::<String>("parent_commit_id")
        .unwrap();
    let fixed_diff_sql = "SELECT id, diff_type, coalesce(to_path, from_path) AS path, from_path, to_path \
         FROM lix_diff('lix_file', $1, $2) \
         ORDER BY coalesce(to_path, from_path), id";
    let fixed_diff_params = [Value::Text(fixed_from), Value::Text(fixed_to)];
    let fixed_diff_expected = authority
        .execute(&fixed_diff_sql, &fixed_diff_params)
        .await
        .unwrap();
    assert!(!fixed_diff_expected.rows().is_empty());
    let diff_values = |result: &ExecuteResult| {
        result
            .rows()
            .iter()
            .map(|row| {
                (
                    row.get::<String>("id").unwrap(),
                    row.get::<String>("diff_type").unwrap(),
                    row.get::<String>("path").unwrap(),
                    row.value("from_path").unwrap().clone(),
                    row.value("to_path").unwrap().clone(),
                )
            })
            .collect::<Vec<_>>()
    };
    for delay_ms in [100_u64, 500] {
        let diff_log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (diff_replica, diff_worker, ..) =
            open_cold_replica_with_delay(diff_log.clone(), delay_ms).await;
        diff_log.lock().unwrap().clear();
        let started = Instant::now();
        let diff_rows = diff_replica
            .execute(&fixed_diff_sql, &fixed_diff_params)
            .await
            .unwrap();
        let cold_ms = started.elapsed().as_secs_f64() * 1000.0;
        let elapsed_budget_ms = if delay_ms == 100 { 3_000.0 } else { 5_000.0 };
        assert_eq!(diff_values(&diff_rows), diff_values(&fixed_diff_expected));
        assert!(
            cold_ms < elapsed_budget_ms,
            "fixed checkpoint Diff exceeded the {elapsed_budget_ms:.0}ms cold latency budget at {delay_ms}ms RTT: {cold_ms:.1}ms"
        );
        let requests = diff_log.lock().unwrap().clone();
        let recipes = fulfillment_requests(&diff_log);
        assert!(
            !recipes.is_empty(),
            "fixed Diff should use typed fulfillment"
        );
        assert!(
            only_read_fulfillment(&diff_log),
            "eligible fixed Diff should not fall back to pointer range requests"
        );
        eprintln!(
            "FIXED_CHECKPOINT_DIFF_PROFILE_JSON={}",
            serde_json::json!({
                "controlled_rtt_ms": delay_ms,
                "rows": diff_rows.rows().len(),
                "cold_ms": cold_ms,
                "elapsed_budget_ms": elapsed_budget_ms,
                "fulfillment_calls": recipes.len(),
                "fulfillment_response_bytes": response_bytes(&recipes),
                "requests": requests,
            })
        );
        diff_log.lock().unwrap().clear();
        let warm = diff_replica
            .execute(&fixed_diff_sql, &fixed_diff_params)
            .await
            .unwrap();
        assert_eq!(diff_values(&warm), diff_values(&fixed_diff_expected));
        assert!(
            diff_log.lock().unwrap().is_empty(),
            "retained fixed checkpoint Diff must execute offline"
        );
        diff_replica.close().await.unwrap();
        diff_worker.abort();
    }

    // Reversed fixed ranges remain public SQL. The authority must decline the
    // bounded recipe and let the original native-demand path produce the same
    // rows instead of treating the optimization's ancestry proof as a query
    // error.
    let reverse_diff_params = [fixed_diff_params[1].clone(), fixed_diff_params[0].clone()];
    let reverse_diff_expected = authority
        .execute(&fixed_diff_sql, &reverse_diff_params)
        .await
        .unwrap();
    let reverse_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (reverse_replica, reverse_worker, ..) = open_cold_replica(reverse_log.clone()).await;
    reverse_log.lock().unwrap().clear();
    let reverse_rows = reverse_replica
        .execute(&fixed_diff_sql, &reverse_diff_params)
        .await
        .unwrap();
    assert_eq!(
        diff_values(&reverse_rows),
        diff_values(&reverse_diff_expected)
    );
    let reverse_requests = reverse_log.lock().unwrap().clone();
    assert!(
        fulfillment_requests(&reverse_log).len() > 0,
        "the bounded optimization should report its ancestry fallback"
    );
    assert!(
        reverse_requests.iter().any(|request| {
            matches!(
                request["operation"].as_str(),
                Some(
                    "native-objects"
                        | "native-object-range"
                        | "native-metadata"
                        | "native-metadata-walk"
                )
            )
        }),
        "a reversed range must retain the original native-demand fallback"
    );
    reverse_replica.close().await.unwrap();
    reverse_worker.abort();

    // Directory history uses the same bounded operation contract, with typed
    // directory identities rather than file IDs. Each checkpoint introduced
    // one directory, so this exercises actual rows rather than an empty scan.
    let directory_sql = sql.replace("'lix_file'", "'lix_directory'");
    let directory_expected = authority.execute(&directory_sql, &params).await.unwrap();
    assert_eq!(directory_expected.rows().len(), 10);
    let directory_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (directory_replica, directory_worker, ..) = open_cold_replica(directory_log.clone()).await;
    directory_log.lock().unwrap().clear();
    let directory_rows = directory_replica
        .execute(&directory_sql, &params)
        .await
        .unwrap();
    assert_eq!(values(&directory_rows), values(&directory_expected));
    assert_eq!(fulfillment_requests(&directory_log).len(), 1);
    assert!(only_read_fulfillment(&directory_log));
    eprintln!(
        "DIRECTORY_HISTORY_CLOSURE_PROFILE_JSON={}",
        serde_json::json!({
            "rows": directory_rows.rows().len(), "requests": directory_log.lock().unwrap().clone(),
        })
    );
    directory_log.lock().unwrap().clear();
    assert_eq!(
        values(
            &directory_replica
                .execute(&directory_sql, &params)
                .await
                .unwrap()
        ),
        values(&directory_expected)
    );
    assert!(directory_log.lock().unwrap().is_empty());
    directory_replica.close().await.unwrap();
    directory_worker.abort();

    // Keep the leased roots fixed while the authority renames a selected file
    // and publishes another checkpoint. History must retain the old paths and
    // source owners rather than discover against the authority's live head.
    let pinned_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (pinned, pinned_worker, ..) = open_cold_replica(pinned_log.clone()).await;
    authority_execute(&server, authority.lix_id(),
        "UPDATE lix_file SET path = '/after-history-lease.txt' WHERE path = '/bounded-history/checkpoint-8/0.txt'", &[]).await;
    authority_execute(
        &server,
        authority.lix_id(),
        "SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)",
        &[],
    )
    .await;
    pinned_log.lock().unwrap().clear();
    let pinned_rows = pinned.execute(&sql, &params).await.unwrap();
    assert_eq!(values(&pinned_rows), values(&expected));
    assert!(
        pinned_rows
            .rows()
            .iter()
            .all(|row| row.get::<String>("path").unwrap() != "/after-history-lease.txt")
    );
    let pinned_requests = fulfillment_requests(&pinned_log);
    assert_eq!(
        pinned_requests.len(),
        1,
        "lease-pinned history should close in one operation"
    );
    assert!(
        only_read_fulfillment(&pinned_log),
        "advancing authority must not restore pointer discovery"
    );
    pinned_log.lock().unwrap().clear();
    assert_eq!(
        values(&pinned.execute(&sql, &params).await.unwrap()),
        values(&expected)
    );
    assert!(pinned_log.lock().unwrap().is_empty());
    pinned.close().await.unwrap();
    pinned_worker.abort();

    // A visible History page starts at the latest checkpoint even when the
    // leased head has subsequently advanced. Scope must be proved from that
    // leased head; requiring anchor == head would miss the actual UI path.
    let historical_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (historical, historical_worker, ..) = open_cold_replica(historical_log.clone()).await;
    historical_log.lock().unwrap().clear();
    assert_eq!(
        values(&historical.execute(&sql, &params).await.unwrap()),
        values(&expected)
    );
    assert_eq!(
        fulfillment_requests(&historical_log).len(),
        1,
        "an older public anchor needs the same bounded closure"
    );
    assert!(only_read_fulfillment(&historical_log));
    historical.close().await.unwrap();
    historical_worker.abort();

    // A local replica can move past the leased public head before querying
    // older public History. The selected commit IDs still prove against that
    // lease, so a private local anchor must not disable the immutable closure.
    let advancing_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (advancing, advancing_worker, _, advancing_state, _, _) =
        open_cold_replica(advancing_log.clone()).await;
    advancing
        .execute("SELECT id, path FROM lix_file ORDER BY path", &[])
        .await
        .unwrap();
    advancing
        .execute(
            "INSERT INTO lix_file(path, content) VALUES ('/unpublished-history-anchor.txt', $1)",
            &[Value::Blob(b"unpublished anchor".to_vec().into())],
        )
        .await
        .unwrap();
    let unpublished_anchor = advancing.create_checkpoint().await.unwrap().commit_id;
    assert_ne!(
        unpublished_anchor,
        advancing_state.descriptor().selected_branch.head.commit_id,
        "fixture must query from a newer local checkpoint than its immutable lease"
    );
    let advancing_params = std::iter::once(Value::Text(unpublished_anchor))
        .chain(selected.iter().cloned().map(Value::Text))
        .collect::<Vec<_>>();
    advancing_log.lock().unwrap().clear();
    let advancing_rows = advancing.execute(&sql, &advancing_params).await.unwrap();
    assert_eq!(values(&advancing_rows), values(&expected));
    let advancing_requests = fulfillment_requests(&advancing_log);
    assert_eq!(
        advancing_requests.len(),
        1,
        "ten public IDs behind an unpublished local anchor should fit one closure"
    );
    assert!(only_read_fulfillment(&advancing_log));
    let physical_after_local_advance = advancing_log
        .lock()
        .unwrap()
        .iter()
        .filter(|request| {
            matches!(
                request["operation"].as_str(),
                Some(
                    "native-objects"
                        | "native-object-range"
                        | "native-metadata"
                        | "native-metadata-walk"
                )
            )
        })
        .count();
    assert_eq!(
        physical_after_local_advance, 0,
        "public History closure must not fall back to pointer reads after a local advance"
    );
    let serialized_history = advancing_requests[0]["request_interests"]
        .as_array()
        .unwrap()
        .iter()
        .find(|interest| interest["kind"] == "history")
        .expect("the bounded History recipe must be sent");
    assert_eq!(
        serialized_history["commit_ids"].as_array().unwrap().len(),
        10
    );
    assert!(
        serialized_history.get("anchor").is_none(),
        "an unpublished local anchor must remain local to SQL planning"
    );
    advancing_log.lock().unwrap().clear();
    assert_eq!(
        values(&advancing.execute(&sql, &advancing_params).await.unwrap()),
        values(&expected)
    );
    assert!(advancing_log.lock().unwrap().is_empty());
    advancing.close().await.unwrap();
    advancing_worker.abort();

    // A local checkpoint cannot be proved on the authority's leased lane.
    // Declining the optimization is normal protocol behavior: retain native
    // demand semantics without an HTTP error or repeated eligibility probes.
    let private_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (private, private_worker, ..) = open_cold_replica(private_log.clone()).await;
    private
        .execute("SELECT id, path FROM lix_file ORDER BY path", &[])
        .await
        .unwrap();
    private
        .execute(
            "INSERT INTO lix_file(path, content) VALUES ('/private-history.txt', $1)",
            &[Value::Blob(b"local checkpoint".to_vec().into())],
        )
        .await
        .unwrap();
    let private_checkpoint = private.create_checkpoint().await.unwrap().commit_id;
    let private_sql = sql.replace("$11) ORDER BY", "$11, $12) ORDER BY");
    let private_params = std::iter::once(Value::Text(private_checkpoint.clone()))
        .chain(selected.iter().cloned().map(Value::Text))
        .chain(std::iter::once(Value::Text(private_checkpoint.clone())))
        .collect::<Vec<_>>();
    private_log.lock().unwrap().clear();
    let private_rows = private
        .execute(&private_sql, &private_params)
        .await
        .unwrap();
    let private_values = values(&private_rows);
    let public_values = private_values
        .iter()
        .filter(|(commit, _, _)| commit != &private_checkpoint)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(public_values, values(&expected));
    let local_values = private_values
        .iter()
        .filter(|(commit, _, _)| commit == &private_checkpoint)
        .collect::<Vec<_>>();
    assert_eq!(local_values.len(), 1);
    assert_eq!(local_values[0].2, "/private-history.txt");
    let private_requests = private_log.lock().unwrap().clone();
    assert!(
        private_requests
            .iter()
            .all(|request| request["status"] == 200)
    );
    assert_eq!(
        fulfillment_requests(&private_log).len(),
        1,
        "unprovable private history must probe eligibility only once per basis"
    );
    let fallback_position = private_requests
        .iter()
        .position(|request| request["operation"] == "read-fulfillment")
        .unwrap();
    assert!(
        private_requests[fallback_position + 1..]
            .iter()
            .any(|request| {
                matches!(
                    request["operation"].as_str(),
                    Some("native-objects" | "native-object-range" | "native-metadata")
                )
            }),
        "private history must exercise native-demand hydration after declining the recipe"
    );
    eprintln!(
        "PRIVATE_HISTORY_FALLBACK_PROFILE_JSON={}",
        serde_json::json!({
            "rows": private_rows.rows().len(), "requests": private_requests,
        })
    );
    private_log.lock().unwrap().clear();
    assert_eq!(
        values(
            &private
                .execute(&private_sql, &private_params)
                .await
                .unwrap()
        ),
        private_values
    );
    assert!(private_log.lock().unwrap().is_empty());
    private.close().await.unwrap();
    private_worker.abort();

    // Drive the real missing-input boundary deterministically. Query planning
    // can choose either side of a mixed History/current-row join first; both
    // orders must allow canonical current-row recovery after private History
    // was found ineligible on the same leased basis.
    let mixed_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (mixed, mixed_worker, mixed_storage, mixed_state, mixed_transport, _) =
        open_cold_replica(mixed_log.clone()).await;
    let capture = crate::hot_state::ReadInterestRegistry::new(16, 65536);
    capture
        .register(crate::hot_state::LogicalReadInterest::History {
            branch_id: mixed_state.descriptor().selected_branch.branch_id.clone(),
            commit_ids: vec![private_checkpoint.clone()],
            relation: "lix_file".into(),
            filter: crate::tracked_state::TrackedStateFilter {
                include_tombstones: true,
                ..Default::default()
            },
            retain_payloads: false,
            projected_columns: vec!["id".into()],
            limit: None,
        })
        .unwrap();
    capture
        .register(crate::hot_state::LogicalReadInterest::Scan {
            request: crate::hot_state::HotStateScanRequest {
                filter: crate::hot_state::HotStateFilter {
                    branch_ids: vec![mixed_state.descriptor().selected_branch.branch_id.clone()],
                    schema_keys: vec!["lix_conversation".into()],
                    row_pks: vec![
                        crate::row_pk::RowPk::uuid_from_canonical(&mixed_conversation_id).unwrap(),
                    ],
                    ..Default::default()
                },
                projection: crate::hot_state::HotStateProjection {
                    columns: vec!["untracked".into(), "raw_snapshot".into()],
                },
                ..Default::default()
            },
            domain: crate::hot_state::InterestDomain::Combined,
        })
        .unwrap();
    mixed_log.lock().unwrap().clear();
    let history_error = crate::sync::read_fulfillment::annotate_capture(
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "missing private History dependency",
        )
        .with_details(
            serde_json::json!({"nativeHistoryDemand": {"version": 1, "includeStateHeaders": true}}),
        ),
        Some(&capture),
    );
    crate::sync::partial_runtime::hydrate_demand_with_receipt(
        &mixed_storage,
        &mixed_state,
        &mixed_transport,
        crate::sync::runtime::SyncDemandRequest::NativeMetadata(
            vec![NativeMetadataRef::CommitGraphRecord(
                checkpoint_ids[0].clone(),
            )],
            history_error,
        ),
    )
    .await
    .unwrap();
    let locator = NativeMetadataRef::ChangeLocator(mixed_conversation_change_id.clone());
    let current_error = crate::sync::read_fulfillment::annotate_capture(
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "selected current change is missing",
        )
        .with_details(serde_json::json!({
            "payloadFailureReason": "selected_change_payload_unavailable",
            "changeId": mixed_conversation_change_id,
        })),
        Some(&capture),
    );
    crate::sync::partial_runtime::hydrate_demand_with_receipt(
        &mixed_storage,
        &mixed_state,
        &mixed_transport,
        crate::sync::runtime::SyncDemandRequest::NativeMetadata(vec![locator], current_error),
    )
    .await
    .unwrap();
    let mixed_recipes = fulfillment_requests(&mixed_log);
    assert_eq!(
        mixed_recipes.len(),
        2,
        "one History fallback and one current-payload recovery"
    );
    assert!(
        mixed_recipes[0]["request_interests"]
            .as_array()
            .unwrap()
            .iter()
            .any(|interest| interest["kind"] == "history")
    );
    assert!(
        mixed_recipes[1]["request_interests"]
            .as_array()
            .unwrap()
            .iter()
            .all(|interest| interest["kind"] != "history")
    );
    assert!(
        mixed_log
            .lock()
            .unwrap()
            .iter()
            .all(|request| request["status"] == 200)
    );
    mixed_log.lock().unwrap().clear();
    let recovered = mixed
        .execute(
            "SELECT title FROM lix_conversation WHERE id = $1 AND lixcol_untracked = false",
            &[Value::Text(mixed_conversation_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(
        recovered.rows()[0].get::<String>("title").unwrap(),
        "Mixed History recovery"
    );
    assert!(
        mixed_log.lock().unwrap().is_empty(),
        "canonical current-row payload must already be installed: {:?}",
        *mixed_log.lock().unwrap()
    );
    mixed.close().await.unwrap();
    mixed_worker.abort();

    // An oversized History selection must not suppress a separately scoped
    // current-row payload demand from the same operation capture.
    let overflow_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (overflow, overflow_worker, overflow_storage, overflow_state, overflow_transport, _) =
        open_cold_replica(overflow_log.clone()).await;
    let overflow_capture = crate::hot_state::ReadInterestRegistry::new(16, 65536);
    let saved_interests = capture.snapshot().unwrap().interests;
    let history_template = saved_interests
        .iter()
        .find(|interest| {
            matches!(
                interest.as_ref(),
                crate::hot_state::LogicalReadInterest::History { .. }
            )
        })
        .unwrap();
    for mask in 1usize..=9 {
        let mut history = history_template.as_ref().clone();
        if let crate::hot_state::LogicalReadInterest::History { commit_ids, .. } = &mut history {
            *commit_ids = selected
                .iter()
                .enumerate()
                .filter(|(index, _)| mask & (1 << index) != 0)
                .map(|(_, id)| id.clone())
                .collect();
        }
        overflow_capture.register(history).unwrap();
    }
    overflow_capture
        .register(
            saved_interests
                .iter()
                .find(|interest| {
                    matches!(
                        interest.as_ref(),
                        crate::hot_state::LogicalReadInterest::Scan { .. }
                    )
                })
                .unwrap()
                .as_ref()
                .clone(),
        )
        .unwrap();
    overflow_capture
        .register(crate::hot_state::LogicalReadInterest::Diff {
            branch_id: Some(
                overflow_state
                    .descriptor()
                    .selected_branch
                    .branch_id
                    .clone(),
            ),
            relation: "lix_file".into(),
            from: crate::hot_state::DiffInterestEndpoint::Fixed(
                overflow_state
                    .descriptor()
                    .selected_branch
                    .head
                    .commit_id
                    .clone(),
            ),
            to: crate::hot_state::DiffInterestEndpoint::ActiveHead,
            filter: crate::tracked_state::TrackedStateFilter::default(),
            retain_payloads: false,
            projected_columns: vec!["id".into()],
            limit: None,
        })
        .unwrap();
    overflow_log.lock().unwrap().clear();
    let history_error = crate::sync::read_fulfillment::annotate_capture(
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "oversized History missing dependency",
        )
        .with_details(
            serde_json::json!({"nativeHistoryDemand": {"version": 1, "includeStateHeaders": true}}),
        ),
        Some(&overflow_capture),
    );
    assert!(
        crate::sync::read_fulfillment::interests_for_error(&history_error)
            .unwrap()
            .is_none()
    );
    crate::sync::partial_runtime::hydrate_demand_with_receipt(
        &overflow_storage,
        &overflow_state,
        &overflow_transport,
        crate::sync::runtime::SyncDemandRequest::NativeMetadata(
            vec![NativeMetadataRef::CommitGraphRecord(
                checkpoint_ids[0].clone(),
            )],
            history_error,
        ),
    )
    .await
    .unwrap();
    assert!(fulfillment_requests(&overflow_log).is_empty());
    overflow_log.lock().unwrap().clear();
    let current_error = crate::sync::read_fulfillment::annotate_capture(
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "selected current change is missing",
        )
        .with_details(serde_json::json!({
            "payloadFailureReason": "selected_change_payload_unavailable",
            "changeId": mixed_conversation_change_id,
        })),
        Some(&overflow_capture),
    );
    crate::sync::partial_runtime::hydrate_demand_with_receipt(
        &overflow_storage,
        &overflow_state,
        &overflow_transport,
        crate::sync::runtime::SyncDemandRequest::NativeMetadata(
            vec![NativeMetadataRef::ChangeLocator(
                mixed_conversation_change_id,
            )],
            current_error,
        ),
    )
    .await
    .unwrap();
    let current_recipes = fulfillment_requests(&overflow_log);
    assert_eq!(
        current_recipes.len(),
        1,
        "oversized History preserves current payload fulfillment"
    );
    assert!(
        current_recipes[0]["request_interests"]
            .as_array()
            .unwrap()
            .iter()
            .all(|interest| interest["kind"] == "scan")
    );
    assert!(
        overflow_log
            .lock()
            .unwrap()
            .iter()
            .all(|request| request["status"] == 200)
    );
    overflow_log.lock().unwrap().clear();
    assert_eq!(
        overflow
            .execute(
                "SELECT title FROM lix_conversation WHERE id = $1 AND lixcol_untracked = false",
                &[Value::Text(mixed_conversation_id)],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("title")
            .unwrap(),
        "Mixed History recovery",
    );
    assert!(
        overflow_log.lock().unwrap().is_empty(),
        "overflow recovery installs the canonical current payload"
    );
    overflow.close().await.unwrap();
    overflow_worker.abort();

    // Keep the old checkpoint chain above as unrelated cold history input,
    // then expose a small active working diff with no new checkpoint. Use the
    // server-owned engine for all authority reads and writes because the
    // original authority handle has stale revision caches after server edits.
    // Compare against the preexisting count so fixture setup cannot hide a
    // change already present in the working set.
    let working_diff_sql = "SELECT count(*) AS file_count FROM lix_diff('lix_file')";
    let authority_working_count =
        |result: &ExecuteResult| result.rows()[0].get::<i64>("file_count").unwrap();
    let before_working_edits =
        authority_execute(&server, authority.lix_id(), working_diff_sql, &[]).await;
    let before_count = authority_working_count(&before_working_edits);
    let new_working_edits = 3i64;
    for edit in 0..new_working_edits {
        authority_execute(
            &server,
            authority.lix_id(),
            "INSERT INTO lix_file(path, content) VALUES ($1, $2)",
            &[
                Value::Text(format!("/bounded-working-diff/new-{edit}.txt")),
                Value::Blob(format!("new active edit {edit}").into_bytes().into()),
            ],
        )
        .await;
    }
    let authority_working =
        authority_execute(&server, authority.lix_id(), working_diff_sql, &[]).await;
    let expected_working_count = authority_working_count(&authority_working);
    assert_eq!(
        expected_working_count - before_count,
        new_working_edits,
        "fixture contributes exactly its known working-file delta"
    );

    let working_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (
        working_replica,
        working_worker,
        _working_storage,
        working_state,
        working_transport,
        working_engine,
    ) = open_cold_replica(working_log.clone()).await;
    working_log.lock().unwrap().clear();
    let working_started = Instant::now();
    let cold_working = working_replica
        .execute(working_diff_sql, &[])
        .await
        .unwrap();
    let cold_working_count = authority_working_count(&cold_working);
    assert_eq!(cold_working_count, expected_working_count);
    let working_requests = working_log.lock().unwrap().clone();
    let working_recipes = fulfillment_requests(&working_log);
    let working_pointer_calls = working_requests
        .iter()
        .filter(|request| {
            matches!(
                request["operation"].as_str(),
                Some(
                    "native-objects"
                        | "native-object-range"
                        | "native-metadata"
                        | "native-metadata-walk"
                )
            )
        })
        .count();
    eprintln!(
        "BOUNDED_WORKING_DIFF_CLOSURE_PROFILE_JSON={}",
        serde_json::json!({
            "decoy_checkpoint_count": checkpoint_ids.len(),
            "preexisting_working_count": before_count,
            "fixture_working_delta": new_working_edits,
            "expected_count": expected_working_count,
            "cold_count": cold_working_count,
            "cold_ms": working_started.elapsed().as_secs_f64() * 1000.0,
            "fulfillment_calls": working_recipes.len(),
            "pointer_calls": working_pointer_calls,
            "requests": working_requests,
        })
    );
    assert_eq!(
        working_recipes.len(),
        1,
        "cold moving working diff should be discovered in one closure"
    );
    assert!(
        only_read_fulfillment(&working_log),
        "cold moving working diff should not fetch native pointers"
    );
    assert_eq!(working_pointer_calls, 0);
    working_log.lock().unwrap().clear();
    assert_eq!(
        authority_working_count(
            &working_replica
                .execute(working_diff_sql, &[])
                .await
                .unwrap()
        ),
        expected_working_count
    );
    assert!(
        working_log.lock().unwrap().is_empty(),
        "retained working-diff closure should serve warm count offline"
    );

    // Keep the moving Diff interest retained while the authority advances its
    // checkpoint and then starts a new active working set. The candidate must
    // use the retained recipe to batch its immutable dependencies before the
    // ordinary publication gate adopts the new descriptor.
    let prior_cursor = working_state.descriptor().cursor;
    authority_execute(
        &server,
        authority.lix_id(),
        "SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)",
        &[],
    )
    .await;
    let next_working_edits = 2i64;
    for edit in 0..next_working_edits {
        authority_execute(
            &server,
            authority.lix_id(),
            "INSERT INTO lix_file(path, content) VALUES ($1, $2)",
            &[
                Value::Text(format!("/bounded-working-diff/next-{edit}.txt")),
                Value::Blob(format!("next active edit {edit}").into_bytes().into()),
            ],
        )
        .await;
    }
    let next_authority_working =
        authority_execute(&server, authority.lix_id(), working_diff_sql, &[]).await;
    let next_expected_count = authority_working_count(&next_authority_working);
    assert_eq!(next_expected_count, next_working_edits);

    working_log.lock().unwrap().clear();
    let next_wrapper = working_transport
        .partial_replica_descriptor(Some(&working_state.descriptor().selected_branch.branch_id))
        .await
        .unwrap();
    assert!(next_wrapper.wire.descriptor.cursor > prior_cursor);
    let next_cursor = next_wrapper.wire.descriptor.cursor;
    let candidate_started = Instant::now();
    let prepared = crate::sync::partial_reconcile::prepare_clean_descriptor(
        working_engine.clone(),
        working_state.clone(),
        &working_transport,
        next_wrapper,
        crate::sync::partial_publication::PartialRecoveryPolicy::Normal,
    )
    .await
    .unwrap();
    let crate::sync::partial_reconcile::PreparedDescriptor::Ready(prepared) = prepared else {
        panic!("advanced authority head should produce a publishable candidate");
    };
    crate::sync::partial_publication::publish_prepared_partial(working_engine.clone(), prepared)
        .await
        .unwrap();
    let adopted = working_engine
        .sync_mode()
        .partial_admission()
        .expect("normal candidate publication keeps replica admission");
    assert_eq!(adopted.descriptor().cursor, next_cursor);
    let candidate_rows = working_replica
        .execute(working_diff_sql, &[])
        .await
        .unwrap();
    assert_eq!(
        authority_working_count(&candidate_rows),
        next_expected_count,
        "retained moving candidate must expose the exact later working count"
    );
    let candidate_requests = working_log.lock().unwrap().clone();
    let candidate_recipes = fulfillment_requests(&working_log);
    let candidate_pointer_calls = candidate_requests
        .iter()
        .filter(|request| {
            matches!(
                request["operation"].as_str(),
                Some(
                    "native-objects"
                        | "native-object-range"
                        | "native-metadata"
                        | "native-metadata-walk"
                )
            )
        })
        .count();
    eprintln!(
        "RETAINED_WORKING_DIFF_CANDIDATE_PROFILE_JSON={}",
        serde_json::json!({
            "candidate_ms": candidate_started.elapsed().as_secs_f64() * 1000.0,
            "previous_cursor": prior_cursor,
            "adopted_cursor": adopted.descriptor().cursor,
            "decoy_checkpoint_count": checkpoint_ids.len(),
            "expected_count": next_expected_count,
            "fulfillment_calls": candidate_recipes.len(),
            "pointer_calls": candidate_pointer_calls,
            "requests": candidate_requests,
        })
    );
    assert_eq!(
        candidate_recipes.len(),
        1,
        "candidate moving working diff should batch into one closure"
    );
    assert_eq!(
        candidate_pointer_calls, 0,
        "candidate should not hydrate serial checkpoint deltas"
    );
    working_log.lock().unwrap().clear();
    assert_eq!(
        authority_working_count(
            &working_replica
                .execute(working_diff_sql, &[])
                .await
                .unwrap()
        ),
        next_expected_count
    );
    assert!(working_log.lock().unwrap().is_empty());
    working_replica.close().await.unwrap();
    working_worker.abort();
}
