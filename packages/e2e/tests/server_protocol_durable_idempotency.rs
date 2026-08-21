use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use http::{Request, StatusCode, header::CONTENT_TYPE};
use http_body_util::BodyExt as _;
use lix::{
    server_protocol::{
        LixServerProtocol, ServerProtocolBody, ServerProtocolContext, ServerProtocolResponse,
    },
    storage::{
        CommitResult, Key, KeyRange, PutBatch, ReadOptions, Storage, StorageError, StorageSpace,
        StorageWrite, WriteOptions,
    },
};
use lix_storage_slatedb::{SlateDB, SlateDBObjectStoreOptions, SlateDBRead, SlateDBWrite};
use object_store::memory::InMemory as ObjectStoreMemory;
use serde_json::{Value, json};

const SESSION_ID_HEADER: &str = "lix-session-id";
const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";

#[derive(Clone)]
struct PostCommitUnknownSlateDB {
    inner: SlateDB,
    fail_next_commit: Arc<AtomicBool>,
}

impl PostCommitUnknownSlateDB {
    fn new() -> Self {
        let inner = SlateDB::open_object_store_with_options(
            "server-protocol-idempotency",
            Arc::new(ObjectStoreMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open SlateDB test storage");
        Self {
            inner,
            fail_next_commit: Arc::new(AtomicBool::new(false)),
        }
    }

    fn fail_next_commit(&self) {
        self.fail_next_commit.store(true, Ordering::Release);
    }
}

struct PostCommitUnknownSlateDBWrite {
    inner: SlateDBWrite,
    fail_after_commit: bool,
}

impl Storage for PostCommitUnknownSlateDB {
    type Read<'a>
        = SlateDBRead
    where
        Self: 'a;
    type Write<'a>
        = PostCommitUnknownSlateDBWrite
    where
        Self: 'a;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.inner.begin_read(options).await
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        Ok(PostCommitUnknownSlateDBWrite {
            inner: self.inner.begin_write(options).await?,
            fail_after_commit: self.fail_next_commit.swap(false, Ordering::AcqRel),
        })
    }
}

impl StorageWrite for PostCommitUnknownSlateDBWrite {
    async fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        self.inner.put_many(space, entries).await
    }

    async fn replace_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        self.inner.replace_many(space, entries).await
    }

    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        let result = self.inner.commit().await?;
        if self.fail_after_commit {
            return Err(StorageError::CommitOutcomeUnknown(
                "injected post-commit acknowledgement loss".to_owned(),
            ));
        }
        Ok(result)
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

async fn request(
    server: &LixServerProtocol<PostCommitUnknownSlateDB>,
    method: &str,
    path: &str,
    session_id: Option<&str>,
    idempotency_key: Option<&str>,
    body: Option<Value>,
) -> ServerProtocolResponse {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(session_id) = session_id {
        builder = builder.header(SESSION_ID_HEADER, session_id);
    }
    if let Some(idempotency_key) = idempotency_key {
        builder = builder.header(IDEMPOTENCY_KEY_HEADER, idempotency_key);
    }
    let body = if let Some(body) = body {
        builder = builder.header(CONTENT_TYPE, "application/json");
        ServerProtocolBody::from(body.to_string())
    } else {
        ServerProtocolBody::empty()
    };
    server
        .handle(
            builder.body(body).expect("protocol request"),
            ServerProtocolContext::anonymous(),
        )
        .await
}

async fn json_body(response: ServerProtocolResponse) -> Value {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    serde_json::from_slice(&bytes).expect("JSON response")
}

async fn open_server() -> (
    PostCommitUnknownSlateDB,
    LixServerProtocol<PostCommitUnknownSlateDB>,
    String,
) {
    let storage = PostCommitUnknownSlateDB::new();
    let lix = Arc::new(
        lix::open_lix()
            .with_storage(storage.clone())
            .as_protocol_root()
            .await
            .expect("open Lix"),
    );
    let server = LixServerProtocol::new(lix);
    let handshake = request(&server, "GET", "/lix/v1", None, None, None).await;
    assert_eq!(handshake.status(), StatusCode::OK);
    let session_id = json_body(handshake).await["sessionId"]
        .as_str()
        .expect("session id")
        .to_owned();
    (storage, server, session_id)
}

#[tokio::test]
async fn idempotency_replays_a_slatedb_post_commit_acknowledgement_loss_once() {
    let (storage, server, session_id) = open_server().await;
    let body = json!({
        "sql": "INSERT INTO lix_key_value (key, value) VALUES ('once', 'persisted')"
    });
    storage.fail_next_commit();
    for _ in 0..2 {
        let response = request(
            &server,
            "POST",
            "/lix/v1/execute",
            Some(&session_id),
            Some("post-commit-ack-loss"),
            Some(body.clone()),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{:?}",
            json_body(response).await
        );
    }
    let count = request(
        &server,
        "POST",
        "/lix/v1/execute",
        Some(&session_id),
        None,
        Some(json!({ "sql": "SELECT COUNT(*) FROM lix_key_value WHERE key = 'once'" })),
    )
    .await;
    assert_eq!(
        json_body(count).await["rows"][0][0],
        json!({ "kind": "int", "value": 1 })
    );
}

#[tokio::test]
async fn batch_idempotency_replays_all_results_after_slatedb_acknowledgement_loss() {
    let (storage, server, session_id) = open_server().await;
    let body = json!({
        "statements": [
            { "sql": "INSERT INTO lix_key_value (key, value) VALUES ('batch-first', 'one')" },
            { "sql": "INSERT INTO lix_key_value (key, value) VALUES ('batch-second', 'two')" }
        ]
    });
    storage.fail_next_commit();
    for _ in 0..2 {
        let response = request(
            &server,
            "POST",
            "/lix/v1/execute-batch",
            Some(&session_id),
            Some("batch-ack-loss"),
            Some(body.clone()),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{:?}",
            json_body(response).await
        );
    }
    let count = request(
        &server,
        "POST",
        "/lix/v1/execute",
        Some(&session_id),
        None,
        Some(json!({
            "sql": "SELECT COUNT(*) FROM lix_key_value WHERE key IN ('batch-first', 'batch-second')"
        })),
    )
    .await;
    assert_eq!(
        json_body(count).await["rows"][0][0],
        json!({ "kind": "int", "value": 2 })
    );
}
