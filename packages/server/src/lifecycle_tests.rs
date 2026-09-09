use crate::{LixRuntimeManager, router, telemetry::InFlightSqlRegistry};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt as _;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tower::ServiceExt as _;

fn empty_host() -> (Arc<LixRuntimeManager>, Router) {
    let manager = LixRuntimeManager::new_in_memory(4);
    let app = router(
        Arc::clone(&manager),
        None,
        Duration::from_secs(60),
        InFlightSqlRegistry::default(),
    );
    (manager, app)
}
async fn create(app: &Router, key: &str, snapshot: Option<Vec<u8>>) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .header(
            lix_sdk::server_protocol::SERVER_PROTOCOL_VERSION_HEADER,
            lix_sdk::server_protocol::PROTOCOL_VERSION,
        )
        .method("POST")
        .uri("/lix/v1")
        .header("idempotency-key", key);
    let body = if let Some(snapshot) = snapshot {
        request = request.header(
            "content-type",
            lix_sdk::server_protocol::SNAPSHOT_MEDIA_TYPE,
        );
        Body::from(snapshot)
    } else {
        Body::empty()
    };
    let response = app
        .clone()
        .oneshot(request.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    (status, value)
}
#[tokio::test]
async fn lifecycle_create_retry_delete_and_missing_open() {
    let (_manager, app) = empty_host();
    let missing = app
        .clone()
        .oneshot(
            Request::builder()
                .header(
                    lix_sdk::server_protocol::SERVER_PROTOCOL_VERSION_HEADER,
                    lix_sdk::server_protocol::PROTOCOL_VERSION,
                )
                .uri("/lix/v1/11111111-1111-4111-8111-111111111111")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    let (status, repo) = create(&app, "first", None).await;
    assert_eq!(status, StatusCode::CREATED, "{repo}");
    let id = repo["id"].as_str().unwrap();
    assert_eq!(repo["url"], format!("http://localhost/lix/{id}"));
    let (status, replay) = create(&app, "first", None).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(repo, replay);
    let opened = app
        .clone()
        .oneshot(
            Request::builder()
                .header(
                    lix_sdk::server_protocol::SERVER_PROTOCOL_VERSION_HEADER,
                    lix_sdk::server_protocol::PROTOCOL_VERSION,
                )
                .uri(format!("/lix/v1/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(opened.status(), StatusCode::OK);
    for _ in 0..2 {
        let deleted = app
            .clone()
            .oneshot(
                Request::builder()
                    .header(
                        lix_sdk::server_protocol::SERVER_PROTOCOL_VERSION_HEADER,
                        lix_sdk::server_protocol::PROTOCOL_VERSION,
                    )
                    .method("DELETE")
                    .uri(format!("/lix/v1/{id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    }
    let missing = app
        .clone()
        .oneshot(
            Request::builder()
                .header(
                    lix_sdk::server_protocol::SERVER_PROTOCOL_VERSION_HEADER,
                    lix_sdk::server_protocol::PROTOCOL_VERSION,
                )
                .uri(format!("/lix/v1/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(create(&app, "first", None).await.0, StatusCode::CONFLICT);
}
#[tokio::test]
async fn lifecycle_snapshot_import_rejects_corruption_and_retries() {
    let (manager, app) = empty_host();
    let (status, _) = create(&app, "snapshot", Some(b"broken snapshot".to_vec())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let source = lix_sdk::open_lix().await.unwrap();
    source
        .execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('hosted-snapshot','preserved')",
            &[],
        )
        .await
        .unwrap();
    source.execute("INSERT INTO lix_key_value (key,value,lixcol_untracked) VALUES ('untracked-app-state','keep-me',true)", &[]).await.unwrap();
    source
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .unwrap();
    let mut snapshot = Vec::new();
    source
        .export_snapshot()
        .write_to(&mut snapshot)
        .await
        .unwrap();
    let (status, repo) = create(&app, "snapshot", Some(snapshot.clone())).await;
    assert_eq!(status, StatusCode::CREATED, "{repo}");
    assert_eq!(
        create(&app, "snapshot", Some(snapshot)).await,
        (StatusCode::CREATED, repo.clone())
    );
    assert_eq!(create(&app, "snapshot", None).await.0, StatusCode::CONFLICT);
    let id = repo["id"].as_str().unwrap();
    let protected = router(
        manager,
        Some("test-token".to_owned()),
        Duration::from_secs(60),
        InFlightSqlRegistry::default(),
    );
    let response = protected
        .oneshot(
            Request::builder()
                .header(
                    lix_sdk::server_protocol::SERVER_PROTOCOL_VERSION_HEADER,
                    lix_sdk::server_protocol::PROTOCOL_VERSION,
                )
                .uri(format!("/lix/v1/{id}/snapshot"))
                .header("authorization", "Bearer test-token")
                .header("x-lix-account-id", "01920000-0000-7000-8000-000000000601")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let restored = lix_sdk::open_lix()
        .from_snapshot(futures_util::io::Cursor::new(bytes))
        .await
        .unwrap();
    let result = restored
        .execute(
            "SELECT value FROM lix_key_value WHERE key='hosted-snapshot'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        result.rows()[0].get::<Value>("value").unwrap(),
        Value::String("preserved".to_owned())
    );
    let untracked = restored
        .execute(
            "SELECT value FROM lix_key_value WHERE key='untracked-app-state'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        untracked.rows()[0].get::<Value>("value").unwrap(),
        Value::String("keep-me".to_owned())
    );
}

#[tokio::test]
async fn local_create_then_sync_uses_original_storage_and_preserves_history() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let mut manager = LixRuntimeManager::new_in_memory(4);
    Arc::get_mut(&mut manager).unwrap().public_url = origin.clone();
    let app = router(
        Arc::clone(&manager),
        Some("integration-token".to_owned()),
        Duration::from_secs(60),
        InFlightSqlRegistry::default(),
    );
    let serving = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let headers = vec![
        (
            "authorization".to_owned(),
            "Bearer integration-token".to_owned(),
        ),
        (
            "x-lix-account-id".to_owned(),
            "01920000-0000-7000-8000-000000000602".to_owned(),
        ),
    ];
    let storage = lix_slatedb_storage::SlateDB::open_object_store_with_options(
        "local-source",
        Arc::new(object_store::memory::InMemory::new()),
        lix_slatedb_storage::SlateDBObjectStoreOptions::default(),
    )
    .unwrap();
    let source = lix_sdk::open_lix()
        .with_storage(storage.clone())
        .await
        .unwrap();
    source
        .execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('local-before-hosting','retained')",
            &[],
        )
        .await
        .unwrap();
    source
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .unwrap();
    let original_history = source
        .execute("SELECT id FROM lix_commit", &[])
        .await
        .unwrap();
    let hosted = lix_sdk::create_lix()
        .with_server(lix_sdk::ServerOptions::new(&origin).with_headers(headers.clone()))
        .from_lix(&source)
        .await
        .expect("upload existing local repository through the public API");
    source.close().await.unwrap();
    let synced = lix_sdk::open_lix()
        .with_storage(storage)
        .with_server(lix_sdk::ServerOptions::new(&hosted.url).with_headers(headers.clone()))
        .await
        .expect("connect the original local storage to its hosted copy");
    let data = synced
        .execute(
            "SELECT value FROM lix_key_value WHERE key='local-before-hosting'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        data.rows()[0].get::<Value>("value").unwrap(),
        Value::String("retained".to_owned())
    );
    let current_history = synced
        .execute("SELECT id FROM lix_commit", &[])
        .await
        .unwrap();
    let ids = current_history
        .rows()
        .iter()
        .map(|row| row.get::<String>("id").unwrap())
        .collect::<std::collections::HashSet<_>>();
    for row in original_history.rows() {
        assert!(ids.contains(&row.get::<String>("id").unwrap()));
    }
    synced.close().await.unwrap();
    lix_sdk::delete_lix()
        .with_server(lix_sdk::ServerOptions::new(&hosted.url).with_headers(headers))
        .await
        .unwrap();
    serving.abort();
    let _ = serving.await;
    manager.shutdown().await.unwrap();
}
