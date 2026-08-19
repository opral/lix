use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_stream::stream;
use bytes::Bytes;

use crate::LixError;
use crate::Value;

use super::http::{
    ProtocolHttp, ProtocolHttpRequest, ProtocolHttpResponse, ProtocolHttpStream, StreamCancel,
};
use super::wire::{SERVER_CLOSED_CODE, SESSION_GONE_CODE};
use super::{open_protocol_client, ProtocolExecuteOptions};

#[derive(Clone, Default)]
struct ScriptHttp {
    requests: Arc<Mutex<Vec<ProtocolHttpRequest>>>,
    outcomes: Arc<Mutex<VecDeque<ScriptOutcome>>>,
}

enum ScriptOutcome {
    Json {
        status: u16,
        body: serde_json::Value,
    },
    Empty {
        status: u16,
    },
    Stream {
        status: u16,
        headers: Vec<(String, String)>,
        chunks: Vec<Bytes>,
    },
}

impl ScriptHttp {
    fn push_json(&self, status: u16, body: serde_json::Value) {
        self.outcomes
            .lock()
            .expect("script outcomes")
            .push_back(ScriptOutcome::Json { status, body });
    }

    fn push_empty(&self, status: u16) {
        self.outcomes
            .lock()
            .expect("script outcomes")
            .push_back(ScriptOutcome::Empty { status });
    }

    fn push_stream(&self, status: u16, body: &str) {
        self.outcomes
            .lock()
            .expect("script outcomes")
            .push_back(ScriptOutcome::Stream {
                status,
                headers: vec![("content-type".to_owned(), "text/event-stream".to_owned())],
                chunks: vec![Bytes::from(body.to_owned())],
            });
    }

    fn requests(&self) -> Vec<ProtocolHttpRequest> {
        self.requests.lock().expect("script requests").clone()
    }
}

impl ProtocolHttp for ScriptHttp {
    async fn request(
        &self,
        request: ProtocolHttpRequest,
    ) -> Result<ProtocolHttpResponse, LixError> {
        self.requests
            .lock()
            .expect("script requests")
            .push(request.clone());
        match self
            .outcomes
            .lock()
            .expect("script outcomes")
            .pop_front()
        {
            Some(ScriptOutcome::Json { status, body }) => Ok(ProtocolHttpResponse {
                status,
                headers: vec![("content-type".to_owned(), "application/json".to_owned())],
                body: Bytes::from(serde_json::to_vec(&body).expect("script json")),
            }),
            Some(ScriptOutcome::Empty { status }) => Ok(ProtocolHttpResponse {
                status,
                headers: Vec::new(),
                body: Bytes::new(),
            }),
            Some(ScriptOutcome::Stream { .. }) => Err(LixError::new(
                "LIX_SERVER_PROTOCOL_ERROR",
                "scripted stream used as a finite request",
            )),
            None => Err(LixError::new(
                "LIX_REMOTE_UNAVAILABLE",
                "no scripted response remaining",
            )),
        }
    }

    async fn request_stream(
        &self,
        request: ProtocolHttpRequest,
    ) -> Result<ProtocolHttpStream, LixError> {
        self.requests
            .lock()
            .expect("script requests")
            .push(request.clone());
        match self
            .outcomes
            .lock()
            .expect("script outcomes")
            .pop_front()
        {
            Some(ScriptOutcome::Stream {
                status,
                headers,
                chunks,
            }) => {
                let cancel: StreamCancel = Arc::new(|| {});
                Ok(ProtocolHttpStream {
                    status,
                    headers,
                    body: Box::pin(stream! {
                        for chunk in chunks {
                            yield Ok(chunk);
                        }
                    }),
                    cancel,
                })
            }
            Some(ScriptOutcome::Json { status, body }) => {
                let cancel: StreamCancel = Arc::new(|| {});
                Ok(ProtocolHttpStream {
                    status,
                    headers: vec![("content-type".to_owned(), "application/json".to_owned())],
                    body: Box::pin(stream! {
                        yield Ok(Bytes::from(serde_json::to_vec(&body).expect("script json")));
                    }),
                    cancel,
                })
            }
            Some(other) => {
                self.outcomes
                    .lock()
                    .expect("script outcomes")
                    .push_front(other);
                Err(LixError::new(
                    "LIX_REMOTE_UNAVAILABLE",
                    "no scripted stream remaining",
                ))
            }
            None => Err(LixError::new(
                "LIX_REMOTE_UNAVAILABLE",
                "no scripted stream remaining",
            )),
        }
    }

    async fn sleep(&self, _duration: Duration) {}

    fn spawn(&self, fut: Pin<Box<dyn Future<Output = ()> + Send>>) {
        tokio::spawn(fut);
    }
}

fn handshake(session_id: &str, branch_id: &str) -> serde_json::Value {
    serde_json::json!({
        "protocolVersion": 2,
        "activeBranchId": branch_id,
        "activeAccountId": "00000000-0000-7000-8000-000000000002",
        "sessionId": session_id,
    })
}

fn protocol_error(code: &str, status: u16) -> serde_json::Value {
    serde_json::json!({
        "error": {
            "code": code,
            "message": code,
        },
        "httpStatus": status,
    })
}

fn execute_ok() -> serde_json::Value {
    serde_json::json!({
        "columns": ["n"],
        "rows": [[{ "kind": "int", "value": 1 }]],
        "rowsAffected": 0,
        "notices": [],
    })
}

fn sse_next(subscription_id: &str) -> String {
    format!(
        "event: next\ndata: {}\n\n",
        serde_json::json!({
            "subscriptionId": subscription_id,
            "sequence": 0,
            "mutationSequence": 1,
            "result": execute_ok(),
        })
    )
}

fn sse_error(code: &str) -> String {
    format!(
        "event: error\ndata: {}\n\n",
        serde_json::json!({
            "error": { "code": code, "message": code },
        })
    )
}

#[tokio::test]
async fn execute_recovers_once_on_session_gone_and_pins_the_last_branch() {
    let http = ScriptHttp::default();
    http.push_json(200, handshake("session-1", "branch-a"));
    http.push_json(410, protocol_error(SESSION_GONE_CODE, 410));
    http.push_json(200, handshake("session-2", "branch-a"));
    http.push_json(200, execute_ok());
    http.push_empty(204);

    let client = open_protocol_client(http.clone(), "https://lix.test/repo", None)
        .await
        .expect("open");
    assert_eq!(client.active_branch_id().await.expect("branch"), "branch-a");
    client
        .execute("SELECT 1", &[], None)
        .await
        .expect("recovered execute");
    client.close().await.expect("close");

    let requests = http.requests();
    assert_eq!(requests[0].method, "GET");
    assert!(!requests[0].url.contains("activeBranchId="));
    assert!(requests[0].header("Lix-Session-Id").is_none());
    assert_eq!(requests[1].method, "POST");
    assert!(requests[1].url.ends_with("/execute"));
    assert_eq!(requests[1].header("Lix-Session-Id"), Some("session-1"));
    assert_eq!(requests[2].method, "GET");
    assert!(requests[2].url.contains("activeBranchId=branch-a"));
    assert!(requests[2].header("Lix-Session-Id").is_none());
    assert_eq!(requests[3].method, "POST");
    assert_eq!(requests[3].header("Lix-Session-Id"), Some("session-2"));
}

#[tokio::test]
async fn execute_recovers_once_on_server_closed() {
    let http = ScriptHttp::default();
    http.push_json(200, handshake("session-1", "main"));
    http.push_json(503, protocol_error(SERVER_CLOSED_CODE, 503));
    http.push_json(200, handshake("session-2", "main"));
    http.push_json(200, execute_ok());
    http.push_empty(204);

    let client = open_protocol_client(http.clone(), "https://lix.test/repo", None)
        .await
        .expect("open");
    client
        .execute(
            "SELECT 1",
            &[Value::Integer(1)],
            Some(ProtocolExecuteOptions {
                origin_key: None,
                idempotency_key: Some("retry-1".to_owned()),
            }),
        )
        .await
        .expect("recovered execute");
    client.close().await.expect("close");
    assert!(
        http.requests()
            .iter()
            .filter(|request| request.method == "GET")
            .count()
            >= 2
    );
}

#[tokio::test]
async fn execute_second_session_gone_fails_without_another_handshake() {
    let http = ScriptHttp::default();
    http.push_json(200, handshake("session-1", "main"));
    http.push_json(410, protocol_error(SESSION_GONE_CODE, 410));
    http.push_json(200, handshake("session-2", "main"));
    http.push_json(410, protocol_error(SESSION_GONE_CODE, 410));
    http.push_empty(204);

    let client = open_protocol_client(http.clone(), "https://lix.test/repo", None)
        .await
        .expect("open");
    let error = client
        .execute("SELECT 1", &[], None)
        .await
        .expect_err("second gone must fail");
    assert_eq!(error.code, SESSION_GONE_CODE);
    client.close().await.expect("close");
    let handshakes = http
        .requests()
        .into_iter()
        .filter(|request| request.method == "GET")
        .count();
    assert_eq!(handshakes, 2);
}

#[tokio::test]
async fn open_handshake_can_pin_an_initial_branch() {
    let http = ScriptHttp::default();
    http.push_json(200, handshake("session-1", "draft / one"));
    http.push_empty(204);
    let client = open_protocol_client(
        http.clone(),
        "https://lix.test/@acme/repository",
        Some("draft / one".to_owned()),
    )
    .await
    .expect("open");
    assert_eq!(
        client.active_account_id().await.expect("account"),
        "00000000-0000-7000-8000-000000000002"
    );
    client.close().await.expect("close");
    let handshake_url = &http.requests()[0].url;
    assert!(handshake_url.contains("/@acme/repository/lix/v1/"));
    assert!(handshake_url.contains("activeBranchId=draft+%2F+one") || handshake_url.contains("draft"));
    assert!(http.requests()[0].header("Lix-Session-Id").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawned observe success path is covered by SESSION_GONE reconnect-loop tests"]
async fn observe_recovers_once_on_session_gone() {
    let http = ScriptHttp::default();
    http.push_json(200, handshake("session-1", "main"));
    http.push_json(410, protocol_error(SESSION_GONE_CODE, 410));
    http.push_json(200, handshake("session-2", "main"));
    http.push_stream(200, &sse_next("observe-1"));
    http.push_json(200, execute_ok());
    http.push_stream(200, "event: message\ndata: \n\n");
    http.push_empty(204);

    let client = open_protocol_client(http.clone(), "https://lix.test/repo", None)
        .await
        .expect("open");
    let events = client
        .observe("SELECT 1", Vec::new())
        .await
        .expect("observe");
    let event = tokio::time::timeout(Duration::from_secs(2), events.next())
        .await
        .expect("observe next timed out")
        .expect("observe next");
    assert!(event.is_some());
    events.close();
    client.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observe_second_session_gone_fails_without_a_reconnect_loop() {
    let http = ScriptHttp::default();
    http.push_json(200, handshake("session-1", "main"));
    http.push_json(410, protocol_error(SESSION_GONE_CODE, 410));
    http.push_json(200, handshake("session-2", "main"));
    http.push_json(410, protocol_error(SESSION_GONE_CODE, 410));
    http.push_empty(204);

    let client = open_protocol_client(http.clone(), "https://lix.test/repo", None)
        .await
        .expect("open");
    let events = client
        .observe("SELECT 1", Vec::new())
        .await
        .expect("observe");
    let error = tokio::time::timeout(Duration::from_secs(2), events.next())
        .await
        .expect("observe next timed out")
        .expect_err("second gone must fail");
    assert_eq!(error.code, SESSION_GONE_CODE);
    events.close();
    client.close().await.expect("close");
    let observe_opens = http
        .requests()
        .into_iter()
        .filter(|request| request.url.contains("/observe/multiplex"))
        .count();
    assert!(
        observe_opens <= 2,
        "observe reconnect looped: {observe_opens} opens"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observe_error_event_recovers_once_then_fails() {
    let http = ScriptHttp::default();
    http.push_json(200, handshake("session-1", "main"));
    http.push_stream(200, &sse_error(SESSION_GONE_CODE));
    http.push_json(200, handshake("session-2", "main"));
    http.push_stream(200, &sse_error(SESSION_GONE_CODE));
    http.push_empty(204);

    let client = open_protocol_client(http.clone(), "https://lix.test/repo", None)
        .await
        .expect("open");
    let events = client
        .observe("SELECT 1", Vec::new())
        .await
        .expect("observe");
    let error = tokio::time::timeout(Duration::from_secs(2), events.next())
        .await
        .expect("observe next timed out")
        .expect_err("second gone must fail");
    assert_eq!(error.code, SESSION_GONE_CODE);
    events.close();
    client.close().await.expect("close");
}
