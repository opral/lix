use super::{OpenOperation, OpenPhase};
use crate::store::{LixRuntimeError, LixRuntimeManager, TestOpenGate};
use opentelemetry::trace::{TraceContextExt as _, TracerProvider as _};
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::Instrument as _;
use tracing::instrument::WithSubscriber as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::layer::SubscriberExt as _;

const TEST_REPOSITORY: &str = "11111111-1111-4111-8111-111111111111";

#[derive(Clone, Debug, Default)]
struct RecordingExporter(Arc<Mutex<Vec<SpanData>>>);

impl SpanExporter for RecordingExporter {
    async fn export(&self, batch: Vec<SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.lock().unwrap().extend(batch);
        Ok(())
    }
}

fn string_attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| attribute.value.as_str().into_owned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn progress_spans_export_before_open_parent_finishes_and_share_operation_id() {
    let exporter = RecordingExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let operation = OpenOperation::new("repository".to_owned(), Duration::from_millis(40));
    let operation_id = operation.snapshot().operation_id;
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("opening-test"))),
    );
    let _default_dispatch = tracing::dispatcher::set_default(&dispatch);
    let parent = tracing::info_span!(
        "lix.runtime.open",
        "lix.open.operation_id" = %operation_id
    );
    let parent_context = parent.context().span().span_context().clone();
    let monitor = tokio::spawn(
        Arc::clone(&operation)
            .monitor()
            .instrument(parent.clone())
            .with_subscriber(dispatch),
    );

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let provider = provider.clone();
            tokio::task::spawn_blocking(move || provider.force_flush())
                .await
                .expect("flush progress span")
                .expect("export progress span");
            let (has_progress, has_stall_error) = {
                let spans = exporter.0.lock().unwrap();
                let has_progress = spans
                    .iter()
                    .any(|span| span.name == "lix.repository.open.progress");
                let has_stall_error = spans.iter().any(|span| {
                    span.name == "lix.repository.open.progress"
                        && string_attribute(span, "error.type").as_deref()
                            == Some("LIX_OPEN_STALLED")
                });
                (has_progress, has_stall_error)
            };
            if has_progress && has_stall_error {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the initial short progress span should export promptly");

    let exported_while_parent_open = exporter.0.lock().unwrap().clone();
    let progress = exported_while_parent_open
        .iter()
        .find(|span| {
            span.name == "lix.repository.open.progress"
                && string_attribute(span, "error.type").is_none()
        })
        .expect("ordinary progress span was exported");
    let stall_error = exported_while_parent_open
        .iter()
        .find(|span| {
            span.name == "lix.repository.open.progress"
                && string_attribute(span, "error.type").as_deref() == Some("LIX_OPEN_STALLED")
        })
        .expect("stalled progress error span was exported");
    assert!(matches!(
        stall_error.status,
        opentelemetry::trace::Status::Error { .. }
    ));
    assert!(
        !exported_while_parent_open
            .iter()
            .any(|span| span.name == "lix.runtime.open"),
        "the open parent must still be unfinished when its progress span exports"
    );
    assert_eq!(progress.parent_span_id, parent_context.span_id());
    assert_eq!(
        string_attribute(progress, "lix.open.operation_id").as_deref(),
        Some(operation_id.as_str())
    );
    assert_eq!(stall_error.parent_span_id, parent_context.span_id());
    assert_eq!(
        string_attribute(stall_error, "lix.open.operation_id").as_deref(),
        Some(operation_id.as_str())
    );
    operation.phase(OpenPhase::Ready, "none");
    tokio::time::timeout(Duration::from_secs(2), monitor)
        .await
        .expect("monitor should stop after terminal progress")
        .expect("monitor task");
    drop(parent);
    provider
        .force_flush()
        .expect("flush completed parent and progress spans");

    let spans = exporter.0.lock().unwrap();
    let parent = spans
        .iter()
        .find(|span| span.name == "lix.runtime.open")
        .expect("completed open parent span");
    assert_eq!(
        string_attribute(parent, "lix.open.operation_id").as_deref(),
        Some(operation_id.as_str())
    );
    assert!(spans.iter().any(|span| {
        span.name == "lix.repository.open.progress"
            && string_attribute(span, "lix.open.phase").as_deref() == Some("ready")
            && string_attribute(span, "lix.open.operation_id").as_deref()
                == Some(operation_id.as_str())
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_open_keeps_one_owned_opener_and_lifecycle_guard_then_resumes() {
    let gate = TestOpenGate {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
        starts: Arc::new(AtomicUsize::new(0)),
        fail_next: Arc::new(AtomicBool::new(false)),
    };
    let mut manager = LixRuntimeManager::new_in_memory(1);
    {
        let manager = Arc::get_mut(&mut manager).expect("manager has one owner before opening");
        manager.open_stall_timeout = Duration::from_secs(1);
        manager.open_gate = Some(gate.clone());
    }
    manager.provision_test_repositories().await;

    let started = gate.started.notified();
    let opener_manager = Arc::clone(&manager);
    let opener = tokio::spawn(async move { opener_manager.get(TEST_REPOSITORY).await });
    started.await;

    let first_result = tokio::time::timeout(Duration::from_secs(3), opener)
        .await
        .expect("the waiter should observe the stall threshold")
        .expect("opening waiter task");
    let first_error = match first_result {
        Err(error) => error,
        Ok(_) => panic!("a stalled open must not admit a runtime"),
    };
    let first_snapshot = match first_error {
        LixRuntimeError::Opening(snapshot) => snapshot,
        other => panic!("expected opening progress, got {other:?}"),
    };
    assert!(first_snapshot.stalled);

    let second_snapshot = match manager.get(TEST_REPOSITORY).await {
        Err(LixRuntimeError::Opening(snapshot)) => snapshot,
        Err(other) => panic!("expected the same opening operation, got {other:?}"),
        Ok(_) => panic!("a stalled repository must not be admitted"),
    };
    assert_eq!(first_snapshot.operation_id, second_snapshot.operation_id);
    assert_eq!(gate.starts.load(Ordering::SeqCst), 1);

    let lifecycle = manager.lifecycle_lock(TEST_REPOSITORY).await;
    assert!(
        lifecycle.try_write().is_err(),
        "the manager-owned opener must retain its lifecycle read guard while stalled"
    );

    gate.release.notify_one();
    let service = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match manager.get(TEST_REPOSITORY).await {
                Ok(service) => break service,
                Err(LixRuntimeError::Opening(_)) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(other) => panic!("the retained opener should resume successfully: {other:?}"),
            }
        }
    })
    .await
    .expect("the original opener should resume after the dependency returns");
    assert_eq!(gate.starts.load(Ordering::SeqCst), 1);
    let lifecycle_write = tokio::time::timeout(Duration::from_secs(1), lifecycle.write())
        .await
        .expect("the lifecycle guard should release after the opener finishes");
    drop(lifecycle_write);
    service.close().await.unwrap();
    manager.shutdown().await.unwrap();
}
