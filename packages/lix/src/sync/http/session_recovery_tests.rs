use super::*;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
struct Client {
    responses: Arc<Mutex<VecDeque<Result<RawHttpResponse, LixError>>>>,
    requests: Arc<Mutex<Vec<RawHttpRequest>>>,
}
impl RawHttpClient for Client {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            self.requests.lock().unwrap().push(request);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected network request")
        })
    }
}
fn response(status: u16, body: serde_json::Value) -> Result<RawHttpResponse, LixError> {
    Ok(RawHttpResponse {
        status,
        status_text: status.to_string(),
        body: serde_json::to_vec(&body).unwrap(),
    })
}
fn gone() -> Result<RawHttpResponse, LixError> {
    response(
        410,
        serde_json::json!({"error":{"code":"LIX_ERROR_PROTOCOL_SESSION_GONE","message":"gone"}}),
    )
}
fn handshake(account: &str) -> Result<RawHttpResponse, LixError> {
    response(
        200,
        serde_json::json!({"protocolVersion":crate::SERVER_PROTOCOL_VERSION,
        "syncProtocolVersion":SYNC_PROTOCOL_VERSION,"lixId":"00000000-0000-4000-8000-000000000001", "sessionId":"replacement", "activeAccountId":account}),
    )
}
fn fixture(responses: Vec<Result<RawHttpResponse, LixError>>) -> HttpSyncTransport<Client> {
    HttpSyncTransport {
        client: Client {
            responses: Arc::new(Mutex::new(responses.into())),
            requests: Default::default(),
        },
        protocol_url: "https://example.test/lix/v1/00000000-0000-4000-8000-000000000001".into(),
        lix_id: "00000000-0000-4000-8000-000000000001".into(),
        active_account_id: crate::SYSTEM_ACCOUNT_ID.into(),
        session: Arc::new(tokio::sync::Mutex::new(SessionState::new("old".into()))),
        baseline_lease: Default::default(),
    }
}
fn request(transport: &HttpSyncTransport<Client>) -> RawHttpRequest {
    let mut request = transport.request(Method::POST, "/sync/push", "push");
    request.body = Some(b"pending edits".to_vec());
    request
}
#[tokio::test]
async fn lost_session_recovers_once_and_preserves_pending_request() {
    let transport = fixture(vec![
        gone(),
        handshake(crate::SYSTEM_ACCOUNT_ID),
        response(200, serde_json::json!({})),
        response(200, serde_json::json!({})),
    ]);
    assert_eq!(
        transport.send(request(&transport)).await.unwrap().status,
        200
    );
    let clone = transport.clone();
    clone.send(request(&clone)).await.unwrap();
    let requests = transport.client.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[1].method, Method::GET);
    assert_eq!(requests[0].body, requests[2].body);
    for i in [2, 3] {
        assert!(
            requests[i]
                .headers
                .iter()
                .any(|(k, v)| k == SESSION_HEADER && v == "replacement")
        );
    }
}
#[tokio::test]
async fn failed_recovery_backs_off_without_resending_stale_session() {
    let transport = fixture(vec![
        gone(),
        response(
            503,
            serde_json::json!({"error":{"code":"UNAVAILABLE","message":"offline"}}),
        ),
        handshake(crate::SYSTEM_ACCOUNT_ID),
        response(200, serde_json::json!({})),
    ]);
    assert!(transport.send(request(&transport)).await.is_err());
    for _ in 0..20 {
        assert!(transport.clone().send(request(&transport)).await.is_err());
    }
    assert_eq!(transport.client.requests.lock().unwrap().len(), 2);
    transport.session.lock().await.retry_at = web_time::Instant::now();
    assert_eq!(
        transport.send(request(&transport)).await.unwrap().status,
        200
    );
}
#[tokio::test]
async fn replacement_rejected_again_is_not_retried_in_a_loop() {
    let transport = fixture(vec![gone(), handshake(crate::SYSTEM_ACCOUNT_ID), gone()]);
    assert_eq!(
        transport.send(request(&transport)).await.unwrap().status,
        410
    );
    for _ in 0..20 {
        assert!(transport.send(request(&transport)).await.is_err());
    }
    assert_eq!(transport.client.requests.lock().unwrap().len(), 3);
}
#[tokio::test]
async fn ambiguous_write_failure_never_replays() {
    let transport = fixture(vec![Err(LixError::unknown("connection lost after commit"))]);
    assert!(transport.send(request(&transport)).await.is_err());
    assert_eq!(transport.client.requests.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn changed_identity_closes_new_session_and_stops_recovery() {
    let transport = fixture(vec![
        gone(),
        handshake("00000000-0000-4000-8000-000000000002"),
        response(204, serde_json::Value::Null),
    ]);
    assert_eq!(
        transport.send(request(&transport)).await.unwrap_err().code,
        super::super::SYNC_PROTOCOL_MISMATCH_CODE
    );
    transport.session.lock().await.retry_at = web_time::Instant::now();
    assert!(transport.send(request(&transport)).await.is_err());
    let requests = transport.client.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[2].method, Method::DELETE);
}
#[tokio::test]
async fn close_never_recovers_and_closed_clones_cannot_reopen() {
    let transport = fixture(vec![gone()]);
    transport.close_session().await.unwrap();
    assert_eq!(
        transport
            .clone()
            .send(request(&transport))
            .await
            .unwrap_err()
            .code,
        LixError::CODE_CLOSED
    );
    assert_eq!(transport.client.requests.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn another_gone_error_never_replays() {
    let transport = fixture(vec![response(
        410,
        serde_json::json!({"error":{"code":"LIX_PARTIAL_BASELINE_EXPIRED","message":"expired"}}),
    )]);
    assert_eq!(
        transport.send(request(&transport)).await.unwrap().status,
        410
    );
    assert_eq!(transport.client.requests.lock().unwrap().len(), 1);
}

#[derive(Clone, Debug)]
struct ConcurrentClient {
    old_requests: Arc<tokio::sync::Barrier>,
    handshakes: Arc<std::sync::atomic::AtomicUsize>,
}
impl RawHttpClient for ConcurrentClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            if request.method == Method::GET {
                self.handshakes
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return handshake(crate::SYSTEM_ACCOUNT_ID);
            }
            if request
                .headers
                .iter()
                .any(|(k, v)| k == SESSION_HEADER && v == "old")
            {
                self.old_requests.wait().await;
                gone()
            } else {
                response(200, serde_json::json!({}))
            }
        })
    }
}
#[tokio::test]
async fn concurrent_stale_clones_share_one_replacement() {
    let transport = HttpSyncTransport {
        client: ConcurrentClient {
            old_requests: Arc::new(tokio::sync::Barrier::new(2)),
            handshakes: Default::default(),
        },
        protocol_url: "https://example.test/lix/v1/00000000-0000-4000-8000-000000000001".into(),
        lix_id: "00000000-0000-4000-8000-000000000001".into(),
        active_account_id: crate::SYSTEM_ACCOUNT_ID.into(),
        session: Arc::new(tokio::sync::Mutex::new(SessionState::new("old".into()))),
        baseline_lease: Default::default(),
    };
    let clone = transport.clone();
    let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        futures_util::join!(
            transport.send(transport.request(Method::POST, "/sync/push", "push")),
            clone.send(clone.request(Method::POST, "/sync/push", "push"))
        )
    })
    .await
    .unwrap();
    assert_eq!(a.unwrap().status, 200);
    assert_eq!(b.unwrap().status, 200);
    assert_eq!(
        transport
            .client
            .handshakes
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

#[derive(Clone, Debug, Default)]
struct SlowClient {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    requests: Arc<std::sync::atomic::AtomicUsize>,
}
impl RawHttpClient for SlowClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            self.requests
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if request.method != Method::GET {
                return gone();
            }
            self.started.notify_one();
            self.release.notified().await;
            response(
                503,
                serde_json::json!({"error":{"code":"UNAVAILABLE","message":"offline"}}),
            )
        })
    }
}
fn slow_transport() -> HttpSyncTransport<SlowClient> {
    HttpSyncTransport {
        client: Default::default(),
        protocol_url: "https://example.test/lix/v1/00000000-0000-4000-8000-000000000001".into(),
        lix_id: "00000000-0000-4000-8000-000000000001".into(),
        active_account_id: crate::SYSTEM_ACCOUNT_ID.into(),
        session: Arc::new(tokio::sync::Mutex::new(SessionState::new("old".into()))),
        baseline_lease: Default::default(),
    }
}
#[tokio::test]
async fn slow_failed_handshake_backs_off_from_failure_not_start() {
    let transport = slow_transport();
    let clone = transport.clone();
    let task = tokio::spawn(async move {
        clone
            .send(clone.request(Method::POST, "/sync/push", "push"))
            .await
    });
    transport.client.started.notified().await;
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    transport.client.release.notify_one();
    assert!(task.await.unwrap().is_err());
    assert!(
        transport
            .send(transport.request(Method::POST, "/sync/push", "push"))
            .await
            .is_err()
    );
    assert_eq!(
        transport
            .client
            .requests
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
}
#[tokio::test]
async fn cancelled_handshake_reserves_retry_backoff() {
    let transport = slow_transport();
    let clone = transport.clone();
    let task = tokio::spawn(async move {
        clone
            .send(clone.request(Method::POST, "/sync/push", "push"))
            .await
    });
    transport.client.started.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(
        transport
            .send(transport.request(Method::POST, "/sync/push", "push"))
            .await
            .is_err()
    );
    assert_eq!(
        transport
            .client
            .requests
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
}
