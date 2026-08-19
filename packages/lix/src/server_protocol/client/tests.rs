use super::{
    OpenRemoteOptions, ProtocolHttp, ProtocolHttpRequest, ProtocolHttpResponse,
    ProtocolHttpStream, ProtocolHttpStreamResponse, RemoteExecuteOptions, SERVER_CLOSED,
    SESSION_GONE, SESSION_ID_HEADER, ServerProtocolClient,
};
use crate::{LixError, Value};
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

struct ChunkStream {
    chunks: std::vec::IntoIter<Vec<u8>>,
}

#[async_trait]
impl ProtocolHttpStream for ChunkStream {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, LixError> {
        Ok(self.chunks.next())
    }
}

#[derive(Clone)]
struct ScriptedHttp {
    calls: Arc<Mutex<Vec<ProtocolHttpRequest>>>,
    handler: Arc<dyn Fn(&ProtocolHttpRequest, usize) -> ScriptedResponse + Send + Sync>,
}

enum ScriptedResponse {
    Json {
        status: u16,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
    },
    Stream {
        status: u16,
        headers: Vec<(String, String)>,
        chunks: Vec<Vec<u8>>,
    },
}

impl ScriptedHttp {
    fn new(
        handler: impl Fn(&ProtocolHttpRequest, usize) -> ScriptedResponse + Send + Sync + 'static,
    ) -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            handler: Arc::new(handler),
        }
    }

    async fn recorded(&self) -> Vec<ProtocolHttpRequest> {
        self.calls.lock().await.clone()
    }
}

#[async_trait]
impl ProtocolHttp for ScriptedHttp {
    async fn request(
        &self,
        request: ProtocolHttpRequest,
    ) -> Result<ProtocolHttpResponse, LixError> {
        let mut calls = self.calls.lock().await;
        let index = calls.len();
        calls.push(request.clone());
        drop(calls);
        match (self.handler)(&request, index) {
            ScriptedResponse::Json {
                status,
                body,
                headers,
            } => Ok(ProtocolHttpResponse {
                status,
                headers,
                body,
            }),
            ScriptedResponse::Stream { .. } => Err(LixError::new(
                "LIX_SERVER_PROTOCOL_ERROR",
                "scripted handler returned a stream for a buffered request",
            )),
        }
    }

    async fn request_stream(
        &self,
        request: ProtocolHttpRequest,
    ) -> Result<ProtocolHttpStreamResponse, LixError> {
        let mut calls = self.calls.lock().await;
        let index = calls.len();
        calls.push(request.clone());
        drop(calls);
        match (self.handler)(&request, index) {
            ScriptedResponse::Stream {
                status,
                headers,
                chunks,
            } => Ok(ProtocolHttpStreamResponse {
                status,
                headers,
                body: Box::new(ChunkStream {
                    chunks: chunks.into_iter(),
                }),
            }),
            ScriptedResponse::Json {
                status,
                body,
                headers,
            } => Ok(ProtocolHttpStreamResponse {
                status,
                headers,
                body: Box::new(ChunkStream {
                    chunks: vec![body].into_iter(),
                }),
            }),
        }
    }

    async fn sleep(&self, _duration: Duration) {}

    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        tokio::spawn(future);
    }
}

fn handshake(session: &str) -> ScriptedResponse {
    json(
        200,
        serde_json::json!({
            "protocolVersion": 2,
            "activeBranchId": "main-id",
            "activeAccountId": "00000000-0000-7000-8000-000000000002",
            "sessionId": session,
        }),
    )
}

fn json(status: u16, value: serde_json::Value) -> ScriptedResponse {
    ScriptedResponse::Json {
        status,
        body: serde_json::to_vec(&value).expect("json"),
        headers: vec![("content-type".into(), "application/json".into())],
    }
}

fn protocol_error(status: u16, code: &str) -> ScriptedResponse {
    json(
        status,
        serde_json::json!({
            "error": { "code": code, "message": code }
        }),
    )
}

fn execute_ok() -> ScriptedResponse {
    json(
        200,
        serde_json::json!({
            "columns": ["n"],
            "rows": [[{ "kind": "int", "value": 1 }]],
            "rowsAffected": 0,
            "notices": [],
        }),
    )
}

fn sse(status: u16, body: &str) -> ScriptedResponse {
    ScriptedResponse::Stream {
        status,
        headers: vec![("content-type".into(), "text/event-stream".into())],
        chunks: vec![body.as_bytes().to_vec()],
    }
}

#[tokio::test]
async fn execute_recovers_once_on_session_gone() {
    let execute_calls = Arc::new(AtomicUsize::new(0));
    let execute_calls_for_handler = Arc::clone(&execute_calls);
    let http = ScriptedHttp::new(move |request, _| {
        if request.method == "GET" {
            let session = request
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(SESSION_ID_HEADER))
                .map_or("session-new", |(_, value)| value.as_str());
            return handshake(if session == "session-new" {
                "session-new"
            } else if request
                .headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case(SESSION_ID_HEADER))
            {
                session
            } else {
                "session-1"
            });
        }
        if request.path == "execute" {
            let count = execute_calls_for_handler.fetch_add(1, Ordering::SeqCst);
            return if count == 0 {
                protocol_error(410, SESSION_GONE)
            } else {
                execute_ok()
            };
        }
        if request.method == "DELETE" {
            return ScriptedResponse::Json {
                status: 204,
                body: Vec::new(),
                headers: Vec::new(),
            };
        }
        panic!("unexpected {}", request.path)
    });
    let client = ServerProtocolClient::open(http.clone(), OpenRemoteOptions::default())
        .await
        .expect("open");
    let result = client
        .execute("SELECT 1", &[], RemoteExecuteOptions::default())
        .await
        .expect("recovered execute");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(execute_calls.load(Ordering::SeqCst), 2);
    let calls = http.recorded().await;
    let handshakes: Vec<_> = calls
        .iter()
        .filter(|call| call.method == "GET")
        .collect();
    assert_eq!(handshakes.len(), 2);
    assert!(
        handshakes[1]
            .headers
            .iter()
            .all(|(name, _)| !name.eq_ignore_ascii_case(SESSION_ID_HEADER)),
        "recover handshake must omit the dead session"
    );
    assert_eq!(
        handshakes[1].query,
        vec![("activeBranchId".into(), "main-id".into())]
    );
    client.close().await.expect("close");
}

#[tokio::test]
async fn execute_recovers_once_on_server_closed() {
    let execute_calls = Arc::new(AtomicUsize::new(0));
    let execute_calls_for_handler = Arc::clone(&execute_calls);
    let http = ScriptedHttp::new(move |request, _| {
        if request.method == "GET" {
            return handshake("session-1");
        }
        if request.path == "execute" {
            let count = execute_calls_for_handler.fetch_add(1, Ordering::SeqCst);
            return if count == 0 {
                protocol_error(503, SERVER_CLOSED)
            } else {
                execute_ok()
            };
        }
        ScriptedResponse::Json {
            status: 204,
            body: Vec::new(),
            headers: Vec::new(),
        }
    });
    let client = ServerProtocolClient::open(http, OpenRemoteOptions::default())
        .await
        .expect("open");
    client
        .execute("SELECT 1", &[], RemoteExecuteOptions::default())
        .await
        .expect("recovered execute");
    assert_eq!(execute_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn second_session_gone_throws_without_another_recover() {
    let handshake_calls = Arc::new(AtomicUsize::new(0));
    let execute_calls = Arc::new(AtomicUsize::new(0));
    let handshake_for_handler = Arc::clone(&handshake_calls);
    let execute_for_handler = Arc::clone(&execute_calls);
    let http = ScriptedHttp::new(move |request, _| {
        if request.method == "GET" {
            handshake_for_handler.fetch_add(1, Ordering::SeqCst);
            return handshake("session-1");
        }
        if request.path == "execute" {
            execute_for_handler.fetch_add(1, Ordering::SeqCst);
            return protocol_error(410, SESSION_GONE);
        }
        ScriptedResponse::Json {
            status: 204,
            body: Vec::new(),
            headers: Vec::new(),
        }
    });
    let client = ServerProtocolClient::open(http, OpenRemoteOptions::default())
        .await
        .expect("open");
    let error = client
        .execute("SELECT 1", &[Value::Integer(1)], RemoteExecuteOptions::default())
        .await
        .expect_err("second 410 must throw");
    assert_eq!(error.code, SESSION_GONE);
    assert_eq!(handshake_calls.load(Ordering::SeqCst), 2);
    assert_eq!(execute_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn observe_recovers_once_then_delivers() {
    let observe_calls = Arc::new(AtomicUsize::new(0));
    let observe_for_handler = Arc::clone(&observe_calls);
    let http = ScriptedHttp::new(move |request, _| {
        if request.method == "GET" {
            return handshake("session-1");
        }
        if request.path == "observe/multiplex" {
            let count = observe_for_handler.fetch_add(1, Ordering::SeqCst);
            if count == 0 {
                return protocol_error(410, SESSION_GONE);
            }
            return sse(
                200,
                "event: next\ndata: {\"subscriptionId\":\"observe-1\",\"sequence\":0,\"mutationSequence\":1,\"result\":{\"columns\":[\"n\"],\"rows\":[[{\"kind\":\"int\",\"value\":7}]],\"rowsAffected\":0,\"notices\":[]}}\n\n",
            );
        }
        if request.path == "execute" {
            return execute_ok();
        }
        ScriptedResponse::Json {
            status: 204,
            body: Vec::new(),
            headers: Vec::new(),
        }
    });
    let client = ServerProtocolClient::open(http, OpenRemoteOptions::default())
        .await
        .expect("open");
    let events = client
        .observe("SELECT 1", &[])
        .await
        .expect("observe");
    let event = tokio::time::timeout(Duration::from_secs(2), events.next())
        .await
        .expect("observe did not hang")
        .expect("event")
        .expect("some");
    assert_eq!(event.mutation_sequence, 1);
    events.close();
}

#[tokio::test]
async fn observe_second_410_fails_without_hanging() {
    let observe_calls = Arc::new(AtomicUsize::new(0));
    let observe_for_handler = Arc::clone(&observe_calls);
    let http = ScriptedHttp::new(move |request, _| {
        if request.method == "GET" {
            return handshake("session-1");
        }
        if request.path == "observe/multiplex" {
            observe_for_handler.fetch_add(1, Ordering::SeqCst);
            return protocol_error(410, SESSION_GONE);
        }
        ScriptedResponse::Json {
            status: 204,
            body: Vec::new(),
            headers: Vec::new(),
        }
    });
    let client = ServerProtocolClient::open(http, OpenRemoteOptions::default())
        .await
        .expect("open");
    let events = client.observe("SELECT 1", &[]).await.expect("observe");
    let error = tokio::time::timeout(Duration::from_secs(2), events.next())
        .await
        .expect("second 410 must not hang")
        .expect_err("second 410 throws");
    assert_eq!(error.code, SESSION_GONE);
    assert!(
        observe_calls.load(Ordering::SeqCst) <= 2,
        "recover-once must not loop observe opens"
    );
}
