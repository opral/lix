//! Ignored ordinary-execute loopback HTTP benchmark for the seven p95 SQL families.
//! Run with `cargo test -p lix-server --lib loopback_execute_http_p95 -- --ignored --nocapture`.
//! Fixture seeding, server/repository opening, handshakes, and session close are outside
//! timed intervals. Each cold sample reopens a fresh server runtime and protocol session.

use super::*;
use base64::Engine as _;
use serde_json::{Value as JsonValue, json};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpListener;

const FILE_COUNT: usize = 128;
const CHECKPOINT_COUNT: usize = 128;
const DIRTY_FILE_COUNT: usize = 1;
const WORKLOAD_LIX_ID: &str = "01940000-0000-7000-8000-000000000101";
const EVICTION_LIX_ID: &str = "01940000-0000-7000-8000-000000000102";
static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct Workload {
    name: &'static str,
    sql: &'static str,
    params: Vec<JsonValue>,
}

fn setting(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().expect("valid benchmark setting"))
        .unwrap_or(default)
}

fn text_param(value: impl Into<String>) -> JsonValue {
    json!({ "kind": "text", "value": value.into() })
}

fn int_param(value: i64) -> JsonValue {
    json!({ "kind": "int", "value": value })
}

async fn seed_workload(manager: &Arc<LixRuntimeManager>) -> String {
    let storage = manager
        .open_storage(WORKLOAD_LIX_ID, SlateDBIoCounters::default())
        .expect("open benchmark seed storage");
    let seed = lix_sdk::open_lix()
        .with_storage(storage)
        .await
        .expect("open benchmark seed engine");

    for index in 0..FILE_COUNT {
        seed.execute(
            "INSERT INTO lix_file (id,path,content) VALUES ($1,$2,$3)",
            &[
                lix_sdk::Value::Text(format!("01940000-0000-7000-8000-{index:012x}")),
                lix_sdk::Value::Text(format!("/directory-{}/file-{index}.txt", index / 8)),
                lix_sdk::Value::Blob(vec![b'x'; 4096].into()),
            ],
        )
        .await
        .expect("seed benchmark file");
    }
    for index in 0..CHECKPOINT_COUNT {
        seed.execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('p95-history',$1) ON CONFLICT (key) DO UPDATE SET value=excluded.value",
            &[lix_sdk::Value::Text(index.to_string())],
        )
        .await
        .expect("seed checkpoint history row");
        seed.execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .expect("seed benchmark checkpoint");
    }
    for index in 0..DIRTY_FILE_COUNT {
        seed.execute(
            "UPDATE lix_file SET content=$2 WHERE id=$1",
            &[
                lix_sdk::Value::Text(format!("01940000-0000-7000-8000-{index:012x}")),
                lix_sdk::Value::Blob(vec![b'y'; 4096].into()),
            ],
        )
        .await
        .expect("seed working diff");
    }
    let account_id = lix_sdk::ANONYMOUS_ACCOUNT_ID.to_owned();
    let account = seed
        .execute(
            "SELECT name, kind, profile_uri FROM lix_account WHERE id = $1",
            &[lix_sdk::Value::Text(account_id.clone())],
        )
        .await
        .expect("read benchmark account witness");
    assert_eq!(account.rows().len(), 1);
    let account_row = &account.rows()[0];
    assert_eq!(account_row.get::<String>("name").unwrap(), "Anonymous");
    assert_eq!(account_row.get::<String>("kind").unwrap(), "anonymous");
    assert!(matches!(
        account_row.value("profile_uri"),
        Ok(lix_sdk::Value::Null)
    ));
    seed.close().await.expect("close benchmark seed engine");
    drop(seed);
    let authority_storage = manager
        .open_storage(WORKLOAD_LIX_ID, SlateDBIoCounters::default())
        .expect("open seeded authority storage");
    let authority = lix_sdk::open_lix()
        .with_storage(authority_storage)
        .serve()
        .with_lix_id(WORKLOAD_LIX_ID)
        .await
        .expect("certify benchmark repository as an authority");
    authority
        .close()
        .await
        .expect("close benchmark authority before adoption");
    drop(authority);
    // Author the fixture through one local storage owner, then adopt that
    // complete repository into the server. Writing through a second SlateDB
    // handle after provisioning can race the authority's tracked-state head.
    manager
        .provision_repository(WORKLOAD_LIX_ID.to_owned(), true)
        .await
        .expect("adopt seeded benchmark repository");
    manager
        .provision_repository(EVICTION_LIX_ID.to_owned(), false)
        .await
        .expect("provision benchmark eviction repository");
    let opened = manager
        .get(WORKLOAD_LIX_ID)
        .await
        .expect("open adopted benchmark repository through the server runtime");
    drop(opened);
    account_id
}

fn workloads(account_id: String) -> Vec<Workload> {
    let target = "01940000-0000-7000-8000-000000000000";
    vec![
        Workload {
            name: "checkpoint_summary",
            sql: "SELECT created_at, count(*) over () AS total_count FROM lix_log() WHERE is_checkpoint ORDER BY position LIMIT 10",
            params: vec![],
        },
        Workload {
            name: "working_diff_count",
            sql: "SELECT count(*) AS file_count FROM lix_diff('lix_file')",
            params: vec![],
        },
        Workload {
            name: "file_content_id",
            sql: "SELECT content FROM lix_file WHERE id = $1",
            params: vec![text_param(target)],
        },
        Workload {
            name: "file_row_id",
            sql: "select id, path, content from lix_file where id = $1 limit $2",
            params: vec![text_param(target), int_param(1)],
        },
        Workload {
            name: "directory_listing",
            sql: "SELECT id, parent_id, path, name, lixcol_updated_at FROM lix_directory ORDER BY path",
            params: vec![],
        },
        Workload {
            name: "account_id",
            sql: "SELECT name, kind, profile_uri FROM lix_account WHERE id = $1",
            params: vec![text_param(account_id)],
        },
        Workload {
            name: "file_paths_ids",
            sql: "select id, path from lix_file where id in ($1)",
            params: vec![text_param(target)],
        },
    ]
}

fn request_id() -> String {
    format!(
        "lix-http-p95-{}",
        REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

fn assert_blob_bytes(value: &JsonValue, expected_byte: u8) {
    assert_eq!(value["kind"], "blob");
    let encoded = value["base64"].as_str().expect("wire blob base64");
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .expect("decode wire blob bytes");
    assert_eq!(decoded, vec![expected_byte; 4096]);
}

fn assert_semantic_witness(query_name: &str, response: &JsonValue) {
    let rows = response["rows"].as_array().expect("response rows");
    match query_name {
        "checkpoint_summary" => {
            assert_eq!(rows.len(), 10);
            let mut timestamps = Vec::with_capacity(rows.len());
            for row in rows {
                assert_eq!(row[1], json!({ "kind": "int", "value": CHECKPOINT_COUNT }));
                let value: lix_sdk::WireValue =
                    serde_json::from_value(row[0].clone()).expect("wire timestamp");
                let lix_sdk::Value::Text(timestamp) = value
                    .try_into_engine()
                    .expect("decode wire checkpoint timestamp")
                else {
                    panic!("checkpoint created_at should be UTF-8 text");
                };
                assert!(timestamp.contains('T'), "checkpoint timestamp should use ISO format");
                timestamps.push(timestamp);
            }
            assert!(
                timestamps.windows(2).all(|pair| pair[0] >= pair[1]),
                "position order should return the nearest checkpoint first"
            );
        }
        "working_diff_count" => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0][0],
                json!({ "kind": "int", "value": DIRTY_FILE_COUNT })
            );
        }
        "file_content_id" => {
            assert_eq!(rows.len(), 1);
            assert_blob_bytes(&rows[0][0], b'y');
        }
        "file_row_id" => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0][0],
                json!({ "kind": "text", "value": "01940000-0000-7000-8000-000000000000" })
            );
            assert_eq!(
                rows[0][1],
                json!({ "kind": "text", "value": "/directory-0/file-0.txt" })
            );
            assert_blob_bytes(&rows[0][2], b'y');
        }
        "directory_listing" => {
            // `open_lix` seeds three tracked repository directories before the
            // fixture adds its sixteen user directories. Assert the complete
            // listing and the stable name/parent relationships independently
            // of the HTTP response oracle.
            let mut expected_paths = vec![
                "/.lix".to_owned(),
                "/.lix/app_data".to_owned(),
                "/.lix/plugins".to_owned(),
            ];
            expected_paths.extend((0..FILE_COUNT / 8).map(|index| format!("/directory-{index}")));
            expected_paths.sort();
            assert_eq!(rows.len(), expected_paths.len());
            assert_eq!(rows.len(), FILE_COUNT / 8 + 3);

            let mut ids = std::collections::HashSet::new();
            let mut id_by_path = HashMap::new();
            let mut parent_by_path = HashMap::new();
            for (row, expected_path) in rows.iter().zip(expected_paths) {
                assert_eq!(
                    row.as_array().expect("directory row columns").len(),
                    5,
                    "directory row should retain all requested columns"
                );
                assert_eq!(row[2], json!({ "kind": "text", "value": expected_path }));
                let id = row[0]["value"].as_str().expect("directory ID is text");
                assert_eq!(row[0]["kind"], "text");
                assert!(!id.is_empty());
                assert!(ids.insert(id.to_owned()), "directory IDs should be unique");
                let path = row[2]["value"].as_str().expect("directory path is text");
                let name = path.rsplit('/').next().expect("directory path has a name");
                assert_eq!(row[3], json!({ "kind": "text", "value": name }));
                let parent_id = if row[1]["kind"] == "null" {
                    None
                } else {
                    assert_eq!(row[1]["kind"], "text");
                    Some(
                        row[1]["value"]
                            .as_str()
                            .expect("directory parent ID is text")
                            .to_owned(),
                    )
                };
                if path.starts_with("/directory-") {
                    assert!(parent_id.is_none(), "fixture directories are root children");
                }
                id_by_path.insert(path.to_owned(), id.to_owned());
                parent_by_path.insert(path.to_owned(), parent_id);
            }

            let root_id = id_by_path.get("/.lix").expect("seeded .lix directory");
            assert_eq!(parent_by_path["/.lix"], None);
            for path in ["/.lix/app_data", "/.lix/plugins"] {
                assert_eq!(
                    parent_by_path[path].as_deref(),
                    Some(root_id.as_str()),
                    "seeded system directory should remain under /.lix",
                );
            }
        }
        "account_id" => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], json!({ "kind": "text", "value": "Anonymous" }));
            assert_eq!(rows[0][1], json!({ "kind": "text", "value": "anonymous" }));
            assert_eq!(rows[0][2], json!({ "kind": "null", "value": null }));
        }
        "file_paths_ids" => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0][0],
                json!({ "kind": "text", "value": "01940000-0000-7000-8000-000000000000" })
            );
            assert_eq!(
                rows[0][1],
                json!({ "kind": "text", "value": "/directory-0/file-0.txt" })
            );
        }
        other => panic!("missing semantic witness for {other}"),
    }
}

async fn handshake(client: &reqwest::Client, base_url: &str, lix_id: &str) -> String {
    let response = client
        .get(format!("{base_url}/lix/v1/{lix_id}/"))
        .header(
            lix_sdk::server_protocol::SERVER_PROTOCOL_VERSION_HEADER,
            lix_sdk::server_protocol::PROTOCOL_VERSION,
        )
        .send()
        .await
        .expect("send benchmark handshake");
    let status = response.status();
    let body = response
        .json::<JsonValue>()
        .await
        .expect("parse benchmark handshake");
    assert!(
        status.is_success(),
        "benchmark handshake returned {status}: {body}"
    );
    body["sessionId"]
        .as_str()
        .expect("handshake session id")
        .to_owned()
}

async fn close_session(client: &reqwest::Client, base_url: &str, lix_id: &str, session_id: &str) {
    let response = client
        .delete(format!("{base_url}/lix/v1/{lix_id}/session"))
        .header(
            lix_sdk::server_protocol::SERVER_PROTOCOL_VERSION_HEADER,
            lix_sdk::server_protocol::PROTOCOL_VERSION,
        )
        .header(lix_sdk::server_protocol::SESSION_ID_HEADER, session_id)
        .send()
        .await
        .expect("send benchmark session close");
    let status = response.status();
    let body = response
        .bytes()
        .await
        .expect("consume benchmark session close body");
    assert!(
        status.is_success(),
        "benchmark session close returned {status}: {}",
        String::from_utf8_lossy(&body)
    );
}

async fn execute_json(
    client: &reqwest::Client,
    base_url: &str,
    session_id: &str,
    query: &Workload,
) -> (f64, JsonValue) {
    let body = serde_json::to_vec(&json!({ "sql": query.sql, "params": query.params }))
        .expect("serialize benchmark execute request");
    let request = client
        .post(format!("{base_url}/lix/v1/{WORKLOAD_LIX_ID}/execute"))
        .header(
            lix_sdk::server_protocol::SERVER_PROTOCOL_VERSION_HEADER,
            lix_sdk::server_protocol::PROTOCOL_VERSION,
        )
        .header(lix_sdk::server_protocol::SESSION_ID_HEADER, session_id)
        .header(
            lix_sdk::server_protocol::IDEMPOTENCY_KEY_HEADER,
            request_id(),
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .build()
        .expect("build benchmark execute request");

    let started = Instant::now();
    let response = client
        .execute(request)
        .await
        .expect("send benchmark execute request");
    let status = response.status();
    let result = response
        .json::<JsonValue>()
        .await
        .expect("parse complete benchmark execute JSON body");
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    assert!(
        status.is_success(),
        "benchmark execute returned {status}: {result}"
    );
    (elapsed_ms, result)
}

async fn reopen_fresh_runtime(client: &reqwest::Client, base_url: &str) -> String {
    // With one runtime slot, opening and closing the decoy evicts the target
    // engine; opening the target evicts the decoy. Both handshakes are outside
    // the timed interval, so the following first SQL request sees a fresh engine.
    let decoy_session = handshake(client, base_url, EVICTION_LIX_ID).await;
    close_session(client, base_url, EVICTION_LIX_ID, &decoy_session).await;
    handshake(client, base_url, WORKLOAD_LIX_ID).await
}

#[tokio::test]
#[ignore = "explicit loopback HTTP performance experiment; emits LIX_HTTP_P95 JSON"]
async fn loopback_execute_http_p95() {
    let trials = setting("LIX_HTTP_P95_TRIALS", 20);
    let warm_samples = setting("LIX_HTTP_P95_WARM_SAMPLES", 100);
    let warmups = 2;
    assert!(trials >= 1 && warm_samples >= 1);

    let manager = LixRuntimeManager::new_in_memory(1);
    let account_id = seed_workload(&manager).await;
    let queries = workloads(account_id);
    let app = crate::router(
        Arc::clone(&manager),
        None,
        Duration::from_secs(120),
        crate::telemetry::InFlightSqlRegistry::default(),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback benchmark listener");
    let address = listener.local_addr().expect("loopback listener address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve loopback benchmark requests");
    });
    let base_url = format!("http://{address}");
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(1)
        .build()
        .expect("build loopback HTTP client");

    // Capture full per-repository response oracles over HTTP before measurement.
    // Each measured trial later reopens the same persisted fixture in a new engine.
    let oracle_session = handshake(&client, &base_url, WORKLOAD_LIX_ID).await;
    let mut oracles = Vec::with_capacity(queries.len());
    for query in &queries {
        let (_, oracle) = execute_json(&client, &base_url, &oracle_session, query).await;
        assert_semantic_witness(query.name, &oracle);
        oracles.push(oracle);
    }
    close_session(&client, &base_url, WORKLOAD_LIX_ID, &oracle_session).await;

    for (query, oracle) in queries.iter().zip(oracles.iter()) {
        let mut trial_samples = Vec::with_capacity(trials);
        for trial_index in 0..trials {
            let session = reopen_fresh_runtime(&client, &base_url).await;
            let (cold, response) = execute_json(&client, &base_url, &session, query).await;
            assert_eq!(
                response, *oracle,
                "cold HTTP response differs for {}",
                query.name
            );
            for _ in 0..warmups {
                let (_, response) = execute_json(&client, &base_url, &session, query).await;
                assert_eq!(
                    response, *oracle,
                    "warmup response differs for {}",
                    query.name
                );
            }
            let mut warm_ms = Vec::with_capacity(warm_samples);
            for _ in 0..warm_samples {
                let (elapsed, response) = execute_json(&client, &base_url, &session, query).await;
                assert_eq!(
                    response, *oracle,
                    "warm HTTP response differs for {}",
                    query.name
                );
                warm_ms.push(elapsed);
            }
            close_session(&client, &base_url, WORKLOAD_LIX_ID, &session).await;
            trial_samples.push(json!({
                "trial": trial_index,
                "cold_ms": cold,
                "warm_ms": warm_ms
            }));
        }
        println!(
            "LIX_HTTP_P95={}",
            json!({
                "query": query.name,
                "execution_kind": "ordinary_execute_http",
                "backend": "in_memory_slatedb",
                "fixture_files": FILE_COUNT,
                "fixture_checkpoints": CHECKPOINT_COUNT,
                "fixture_dirty_files": DIRTY_FILE_COUNT,
                "trials": trials,
                "warmups_per_trial": warmups,
                "warm_samples_per_trial": warm_samples,
                "trial_source": "LIX_HTTP_P95_TRIALS",
                "warm_sample_source": "LIX_HTTP_P95_WARM_SAMPLES",
                "trial_samples": trial_samples,
                "cold_boundary": "fresh_runtime_engine_and_protocol_session",
                "timing_scope": "loopback_http_request_send_through_complete_json_body_parse",
                "setup_scope": "fixture_seed_repository_open_handshake_and_session_close_excluded",
                "oracle": oracle,
                "verified_every_response": true
            })
        );
    }

    manager.shutdown().await.expect("close benchmark runtimes");
    server.abort();
    let _ = server.await;
}
