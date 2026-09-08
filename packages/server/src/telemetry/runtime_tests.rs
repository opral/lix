use super::*;
use axum::{
    Router,
    body::{Body, Bytes},
    http::{HeaderMap, Request, StatusCode},
    routing::post,
};
use http_body_util::BodyExt as _;
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest, trace::v1::Span,
};
use prost::Message as _;
use serde_json::{Value, json};
use std::io::Read as _;
use tower::ServiceExt as _;

const LIX_ID: &str = "11111111-1111-4111-8111-111111111111";
const ACCOUNT_ID: &str = "22222222-2222-4222-8222-222222222222";

async fn flush(provider: &SdkTracerProvider) {
    let provider = provider.clone();
    tokio::task::spawn_blocking(move || provider.force_flush())
        .await
        .unwrap()
        .unwrap();
}

fn sink(provider: &SdkTracerProvider) -> Arc<dyn TelemetrySink> {
    let subscriber = tracing_subscriber::registry().with(
        tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("lix-server"))
            .with_filter(EnvFilter::new(OTEL_TELEMETRY_FILTER)),
    );
    Arc::new(OpenTelemetryTracingSink::new(tracing::Dispatch::new(
        subscriber,
    )))
}

async fn handshake(app: &Router, account: Option<&str>, session: Option<&str>) -> String {
    let mut request = Request::builder()
        .uri(format!("/lix/v1/{LIX_ID}/"))
        .header("authorization", "Bearer test-internal-token");
    if let Some(account) = account {
        request = request.header("x-lix-account-id", account);
    }
    if let Some(session) = session {
        request = request.header(lix_sdk::server_protocol::SESSION_ID_HEADER, session);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    body["sessionId"].as_str().unwrap().to_owned()
}

fn attribute<'a>(span: &'a Span, key: &str) -> Option<&'a str> {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    span.attributes
        .iter()
        .find(|a| a.key == key)
        .and_then(|a| a.value.as_ref())
        .and_then(|v| match v.value.as_ref()? {
            Value::StringValue(v) => Some(v.as_str()),
            _ => None,
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_protocol_bind_reaches_otlp() {
    let spans = Arc::new(Mutex::new(Vec::<Span>::new()));
    let received = Arc::clone(&spans);
    let mock = Router::new().route(
        "/v1/traces",
        post(move |headers: HeaderMap, body: Bytes| {
            let received = Arc::clone(&received);
            async move {
                assert_eq!(headers["content-encoding"], "gzip");
                assert_eq!(headers["content-type"], "application/x-protobuf");
                let mut decoded = Vec::new();
                flate2::read::GzDecoder::new(body.as_ref())
                    .read_to_end(&mut decoded)
                    .unwrap();
                let request = ExportTraceServiceRequest::decode(decoded.as_slice()).unwrap();
                for resource in request.resource_spans {
                    assert!(
                        resource
                            .resource
                            .as_ref()
                            .unwrap()
                            .attributes
                            .iter()
                            .any(|a| a.key == "service.name")
                    );
                    for scope in resource.scope_spans {
                        received.lock().unwrap().extend(scope.spans);
                    }
                }
                StatusCode::OK
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1/traces", listener.local_addr().unwrap());
    let mock_task = tokio::spawn(async move { axum::serve(listener, mock).await });
    let provider = provider_from_endpoint(Some(endpoint)).unwrap();
    let manager = crate::LixRuntimeManager::new_in_memory_with_telemetry(1, sink(&provider));
    manager.provision_test_repositories().await;
    let app = crate::router(
        Arc::clone(&manager),
        Some("test-internal-token".into()),
        Duration::from_secs(60),
        InFlightSqlRegistry::default(),
    );
    drop(manager.get(LIX_ID).await.unwrap());
    flush(&provider).await;
    assert!(
        spans
            .lock()
            .unwrap()
            .iter()
            .all(|s| s.name != "lix.repository.opened")
    );
    let session = handshake(&app, Some(ACCOUNT_ID), None).await;
    assert_eq!(
        handshake(&app, Some(ACCOUNT_ID), Some(&session)).await,
        session
    );
    handshake(&app, None, None).await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/lix/v1/{LIX_ID}/execute"))
                .header("authorization", "Bearer test-internal-token")
                .header("x-lix-account-id", ACCOUNT_ID)
                .header(lix_sdk::server_protocol::SESSION_ID_HEADER, &session)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"sql":"SELECT 1","params":[]}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await.unwrap();
    flush(&provider).await;
    {
        let spans = spans.lock().unwrap();
        let opens: Vec<_> = spans
            .iter()
            .filter(|s| s.name == "lix.repository.opened")
            .collect();
        assert_eq!(
            opens.len(),
            2,
            "identified and anonymous binds, no duplicate on resume"
        );
        assert_eq!(
            opens
                .iter()
                .filter(|s| attribute(s, "lix.account_id") == Some(ACCOUNT_ID))
                .count(),
            1
        );
        // The engine's persisted Lix ID can differ from the host's storage key.
        let engine_id = attribute(opens[0], "lix.id").expect("engine identity");
        uuid::Uuid::parse_str(engine_id).expect("valid engine identity");
        for span in opens {
            assert_eq!(attribute(span, "lix.id"), Some(engine_id));
            assert!(attribute(span, "lix.branch_id").is_some());
            assert_eq!(span.trace_id.len(), 16);
            assert_eq!(span.span_id.len(), 8);
            assert!(span.start_time_unix_nano > 0);
        }
        assert!(spans.iter().any(|s| s.name == "lix.sql.query"));
    }
    manager.shutdown().await.unwrap();
    tokio::task::spawn_blocking(move || provider.shutdown())
        .await
        .unwrap()
        .unwrap();
    mock_task.abort();
}

// Isolate environment configuration from parallel tests in a fresh process.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn otlp_environment_headers_reach_receiver() {
    const CHILD: &str = "LIX_TEST_OTLP_ENV_CHILD";
    if env::var_os(CHILD).is_some() {
        let provider = provider_from_env().unwrap();
        lix_sdk::bind_session(Some(&sink(&provider)), LIX_ID, "branch", Some(ACCOUNT_ID));
        flush(&provider).await;
        tokio::task::spawn_blocking(move || provider.shutdown())
            .await
            .unwrap()
            .unwrap();
        return;
    }
    let received = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let handler_received = Arc::clone(&received);
    let mock = Router::new().route(
        "/v1/traces",
        post(move |headers: HeaderMap, _body: Bytes| {
            let received = Arc::clone(&handler_received);
            async move {
                assert_eq!(headers["authorization"], "Bearer test-collector-token");
                received.store(true, std::sync::atomic::Ordering::SeqCst);
                StatusCode::OK
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1/traces", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, mock).await });
    let status = tokio::task::spawn_blocking(move || {
        std::process::Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "telemetry::runtime_tests::otlp_environment_headers_reach_receiver",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", endpoint)
            .env(
                "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
                "Authorization=Bearer%20test-collector-token",
            )
            .status()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(status.success());
    assert!(received.load(std::sync::atomic::Ordering::SeqCst));
    task.abort();
}
