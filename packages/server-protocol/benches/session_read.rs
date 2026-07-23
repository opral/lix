use std::{hint::black_box, sync::Arc};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header::CONTENT_TYPE},
};
use criterion::{Criterion, criterion_group, criterion_main};
use lix_sdk::{OpenLixOptions, Value, open_lix};
use lix_server_protocol::{LixProtocolServer, SESSION_ID_HEADER, handler};
use serde_json::{Value as JsonValue, json};
use tower::ServiceExt;

const FILE_PATH: &str = "/protocol-session-bench.bin";
const RESPONSE_LIMIT: usize = 16 * 1024;

fn bench_session_read(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build benchmark runtime");
    let root = Arc::new(
        runtime
            .block_on(open_lix(OpenLixOptions::default()))
            .expect("open benchmark workspace"),
    );
    runtime
        .block_on(root.execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[
                Value::Text(FILE_PATH.to_string()),
                Value::Blob(vec![0x5a; 2_048]),
            ],
        ))
        .expect("seed benchmark file");
    let router = handler(LixProtocolServer::new(root));
    let session_id = runtime.block_on(open_session(&router));
    let request_body = serde_json::to_vec(&json!({
        "sql": "SELECT data FROM lix_file WHERE path = $1",
        "params": [{ "kind": "text", "value": FILE_PATH }],
    }))
    .expect("encode execute request");
    runtime.block_on(verify_read(&router, &session_id, request_body.clone()));

    let mut group = c.benchmark_group("in_process_protocol_exact_file_read_2k");
    group.bench_function("default_remote_session", |b| {
        b.iter(|| {
            let response = runtime
                .block_on(
                    router
                        .clone()
                        .oneshot(execute_request(&session_id, request_body.clone())),
                )
                .expect("execute protocol request");
            assert_eq!(response.status(), StatusCode::OK);
            let body = runtime
                .block_on(to_bytes(response.into_body(), RESPONSE_LIMIT))
                .expect("read execute response");
            black_box(body);
        });
    });
    group.finish();
}

fn execute_request(session_id: &str, body: Vec<u8>) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/lix/v1/execute")
        .header(SESSION_ID_HEADER, session_id)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .expect("build execute request")
}

async fn verify_read(router: &Router, session_id: &str, request_body: Vec<u8>) {
    let response = router
        .clone()
        .oneshot(execute_request(session_id, request_body))
        .await
        .expect("verify execute request");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), RESPONSE_LIMIT)
        .await
        .expect("read verified response");
    let value: JsonValue = serde_json::from_slice(&body).expect("decode verified response");
    assert_eq!(value["rows"][0][0]["kind"], "blob");
    assert_eq!(
        value["rows"][0][0]["base64"]
            .as_str()
            .expect("verified blob")
            .len(),
        2_732
    );
}

async fn open_session(router: &Router) -> String {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/lix/v1")
                .body(Body::empty())
                .expect("build handshake request"),
        )
        .await
        .expect("handshake");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), RESPONSE_LIMIT)
        .await
        .expect("read handshake response");
    let value: JsonValue = serde_json::from_slice(&body).expect("decode handshake response");
    value["sessionId"]
        .as_str()
        .expect("handshake session id")
        .to_string()
}

criterion_group!(benches, bench_session_read);
criterion_main!(benches);
