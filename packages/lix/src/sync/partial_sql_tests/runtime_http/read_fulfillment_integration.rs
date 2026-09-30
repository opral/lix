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

#[derive(Clone)]
struct ReadOnlyProductionClient {
    client: reqwest::Client,
    repo_path: String,
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
                "GET" => {
                    route.is_empty() || route == "/" || route == "/sync/descriptor"
                }
                "POST" => [
                    "/sync/read-fulfillment",
                    "/sync/native-objects",
                    "/sync/native-object-range",
                    "/sync/native-metadata",
                    "/sync/native-metadata-walk",
                ]
                .iter()
                .any(|suffix| route == *suffix),
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
    let normalized_url = url::Url::parse(&normalized.protocol_url)
        .expect("normalized probe URL should parse");
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
    assert!(close_result.is_ok(), "production HTTP session cleanup failed");
}

async fn run_production_cold_global_probe(
    transport: &HttpSyncTransport<ReadOnlyProductionClient>,
) -> Result<(), LixError> {
    let leased = transport.partial_replica_descriptor(None).await?;
    let state = Arc::new(
        PartialReplicaState::from_leased(
            transport.protocol_url().into(),
            transport.active_account_id().into(),
            uuid::Uuid::now_v7().to_string(),
            leased.wire,
        )?,
    );
    transport.bind_native_baseline_lease(state.baseline_lease())?;

    let storage = StorageAdapter::new(
        crate::storage_adapter::StorageSession::acquire(Memory::new()).await?,
    );
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
        Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &state)
            .await
            ?;
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
