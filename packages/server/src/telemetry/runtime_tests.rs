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
        .header(
            lix_sdk::server_protocol::SERVER_PROTOCOL_VERSION_HEADER,
            lix_sdk::server_protocol::PROTOCOL_VERSION,
        )
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

fn is_select_span(span: &Span) -> bool {
    span.name == "SELECT"
        && attribute(span, "db.system.name") == Some("lix")
        && attribute(span, "db.operation.name") == Some("SELECT")
}

fn is_select_span_data(span: &opentelemetry_sdk::trace::SpanData) -> bool {
    let has_attribute = |key: &str| {
        span.attributes
            .iter()
            .any(|attribute| attribute.key.as_str() == key && attribute.value.as_str() == "SELECT")
    };
    span.name == "SELECT"
        && span.attributes.iter().any(|attribute| {
            attribute.key.as_str() == "db.system.name" && attribute.value.as_str() == "lix"
        })
        && has_attribute("db.operation.name")
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
                .header(
                    lix_sdk::server_protocol::SERVER_PROTOCOL_VERSION_HEADER,
                    lix_sdk::server_protocol::PROTOCOL_VERSION,
                )
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
        assert!(spans.iter().any(is_select_span));
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

#[derive(Debug, Clone, Default)]
struct RecordingExporter(Arc<Mutex<Vec<opentelemetry_sdk::trace::SpanData>>>);

impl SpanExporter for RecordingExporter {
    async fn export(
        &self,
        batch: Vec<opentelemetry_sdk::trace::SpanData>,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.lock().unwrap().extend(batch);
        Ok(())
    }
}

#[tokio::test]
async fn protocol_requests_export_remote_parents_without_cross_request_context() {
    use tracing::instrument::WithSubscriber as _;
    let exporter = RecordingExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test"))),
    );
    let manager = crate::LixRuntimeManager::new_in_memory_with_telemetry(1, sink(&provider));
    let app = crate::router(
        manager,
        Some("test-internal-token".into()),
        Duration::from_secs(60),
        InFlightSqlRegistry::default(),
    );
    let request = |traceparent: &'static str| {
        let app = app.clone();
        async move {
            let response = app
                .oneshot(
                    Request::builder()
                        .uri(format!("/lix/v1/{LIX_ID}/execute"))
                        .header("traceparent", traceparent)
                        .header("tracestate", "vendor=opaque")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            response.into_body().collect().await.unwrap();
        }
        .with_subscriber(dispatch.clone())
    };
    tokio::join!(
        request("00-11111111111111111111111111111111-aaaaaaaaaaaaaaaa-01"),
        request("00-22222222222222222222222222222222-bbbbbbbbbbbbbbbb-01"),
        request("invalid"),
        request("00-33333333333333333333333333333333-cccccccccccccccc-00"),
    );
    provider.force_flush().unwrap();
    let spans = exporter.0.lock().unwrap().clone();
    let requests: Vec<_> = spans
        .iter()
        .filter(|s| s.name == "Lix protocol request")
        .collect();
    assert_eq!(
        requests.len(),
        3,
        "unsampled remote parent remains unsampled"
    );
    for (trace, parent) in [
        ("11111111111111111111111111111111", "aaaaaaaaaaaaaaaa"),
        ("22222222222222222222222222222222", "bbbbbbbbbbbbbbbb"),
    ] {
        let span = requests
            .iter()
            .find(|s| s.span_context.trace_id().to_string() == trace)
            .unwrap();
        assert_eq!(span.parent_span_id.to_string(), parent);
        assert!(span.parent_span_is_remote);
        assert_eq!(span.span_context.trace_state().header(), "vendor=opaque");
    }
    let root = requests
        .iter()
        .find(|s| s.parent_span_id == opentelemetry::trace::SpanId::INVALID)
        .unwrap();
    assert!(root.span_context.is_valid());
    assert!(!root.parent_span_is_remote);
}

#[tokio::test]
async fn protocol_handshake_and_sql_remain_in_remote_trace() {
    use tracing::instrument::WithSubscriber as _;
    let exporter = RecordingExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test"))),
    );
    let manager = crate::LixRuntimeManager::new_in_memory_with_telemetry(
        1,
        Arc::new(OpenTelemetryTracingSink::new(dispatch.clone())),
    );
    manager.provision_test_repositories().await;
    let app = crate::router(
        manager.clone(),
        Some("test-internal-token".into()),
        Duration::from_secs(60),
        InFlightSqlRegistry::default(),
    );
    async {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/lix/v1/{LIX_ID}/"))
                    .header("authorization", "Bearer test-internal-token")
                    .header(
                        lix_sdk::server_protocol::SERVER_PROTOCOL_VERSION_HEADER,
                        lix_sdk::server_protocol::PROTOCOL_VERSION,
                    )
                    .header(
                        "traceparent",
                        "00-44444444444444444444444444444444-dddddddddddddddd-01",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/lix/v1/{LIX_ID}/execute"))
                    .header("authorization", "Bearer test-internal-token")
                    .header(
                        lix_sdk::server_protocol::SERVER_PROTOCOL_VERSION_HEADER,
                        lix_sdk::server_protocol::PROTOCOL_VERSION,
                    )
                    .header(
                        lix_sdk::server_protocol::SESSION_ID_HEADER,
                        body["sessionId"].as_str().unwrap(),
                    )
                    .header("content-type", "application/json")
                    .header(
                        "traceparent",
                        "00-44444444444444444444444444444444-eeeeeeeeeeeeeeee-01",
                    )
                    .body(Body::from(
                        json!({"sql":"SELECT 1","params":[]}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        response.into_body().collect().await.unwrap();
    }
    .with_subscriber(dispatch)
    .await;
    provider.force_flush().unwrap();
    {
        let spans = exporter.0.lock().unwrap().clone();
        let sql = spans
            .iter()
            .find(|span| is_select_span_data(span))
            .expect("engine SQL span");
        assert_eq!(
            sql.span_context.trace_id().to_string(),
            "44444444444444444444444444444444"
        );
        let mut parent_id = sql.parent_span_id;
        while let Some(parent) = spans.iter().find(|s| s.span_context.span_id() == parent_id) {
            assert_eq!(parent.span_context.trace_id(), sql.span_context.trace_id());
            parent_id = parent.parent_span_id;
        }
        assert_eq!(parent_id.to_string(), "eeeeeeeeeeeeeeee");
    }
    manager.shutdown().await.unwrap();
}

/// Opt-in cross-repository contract: LIX_TRACE_NODE_FIXTURE points to LixRay's
/// web-app/scripts/trace-integration.mjs. It uses the real JS SDK over TCP.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires LixRay checkout with installed Node dependencies and built SDK"]
async fn node_client_trace_integration() {
    use tracing::instrument::WithSubscriber as _;
    let fixture = env::var("LIX_TRACE_NODE_FIXTURE").expect("set LIX_TRACE_NODE_FIXTURE");
    let exporter = RecordingExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("integration"))),
    );
    tracing::dispatcher::set_global_default(dispatch.clone()).expect("run this opt-in test alone");
    let manager = crate::LixRuntimeManager::new_in_memory_with_telemetry(
        1,
        Arc::new(OpenTelemetryTracingSink::new(dispatch.clone())),
    );
    manager.provision_test_repositories().await;
    let app = crate::router(
        manager.clone(),
        Some("test-internal-token".into()),
        Duration::from_secs(60),
        InFlightSqlRegistry::default(),
    )
    .layer(axum::middleware::from_fn(
        move |request: Request<Body>, next: axum::middleware::Next| {
            next.run(request).with_subscriber(dispatch.clone())
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("node")
            .arg("--experimental-strip-types")
            .arg(fixture)
            .env("LIX_TRACE_SERVER_URL", address)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    server.abort();
    assert!(
        output.status.success(),
        "Node fixture failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let client_spans: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    provider.force_flush().unwrap();
    {
        let spans = exporter.0.lock().unwrap().clone();
        let requests: Vec<_> = spans
            .iter()
            .filter(|s| s.name == "Lix protocol request")
            .collect();
        assert!(
            requests.len() >= 8,
            "expected handshake, provisioning, SQL, transaction and close; got: {:?}",
            requests.iter().map(|s| &s.attributes).collect::<Vec<_>>()
        );
        for path in [
            "",
            "transaction/begin",
            "transaction/execute",
            "transaction/commit",
            "session",
        ] {
            assert!(
                requests.iter().any(|s| s
                    .attributes
                    .iter()
                    .any(|a| a.key.as_str() == "lix.protocol.path" && a.value.as_str() == path)),
                "missing protocol phase {path}"
            );
        }
        assert_eq!(
            requests.len(),
            client_spans
                .iter()
                .filter(|s| s["name"] == "lix.client.rpc")
                .count()
        );
        for request in &requests {
            let client = client_spans
                .iter()
                .find(|s| s["spanId"] == request.parent_span_id.to_string())
                .expect("every Rust server span is parented by a real JS CLIENT span");
            assert_eq!(client["name"], "lix.client.rpc");
            assert_eq!(
                client["traceId"],
                request.span_context.trace_id().to_string()
            );
            assert!(request.parent_span_is_remote);
        }
        let tool = client_spans
            .iter()
            .find(|s| s["name"] == "integration.mcp.tool")
            .unwrap();
        for request in &requests {
            assert_eq!(tool["traceId"], request.span_context.trace_id().to_string());
        }
        assert!(spans.iter().any(|span| is_select_span_data(span)
            && span.span_context.trace_id().to_string() == tool["traceId"].as_str().unwrap()));
        assert!(spans.iter().any(|s| s.name.contains("checkpoint")
            && s.span_context.trace_id().to_string() == tool["traceId"].as_str().unwrap()));
        eprintln!(
            "Connected {} JS spans and {} Rust spans ({} protocol requests)",
            client_spans.len(),
            spans.len(),
            requests.len()
        );
    }
    manager.shutdown().await.unwrap();
}
