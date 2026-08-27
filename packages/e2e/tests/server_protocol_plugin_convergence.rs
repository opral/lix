use std::io::{Cursor, Write as _};
use std::path::Path;
use http::{Request, StatusCode, header::CONTENT_TYPE};
use http_body_util::BodyExt as _;
use lix::server_protocol::{
    LixServerProtocol, SESSION_ID_HEADER, ServerProtocolBody, ServerProtocolContext,
    ServerProtocolResponse, TRANSACTION_ID_HEADER,
};
use lix::{Memory, Value, open_lix};
use serde_json::{Value as JsonValue, json};

#[tokio::test]
async fn same_base_server_protocol_plugin_writes_resolve_and_converge() {
    let storage = Memory::new();
    let setup = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open setup Lix");
    setup.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
        &[
            Value::Text("/.lix/plugins/plugin_json.lixplugin".to_owned()),
            Value::Blob(json_plugin_archive().into()),
        ],
    )
    .await
    .expect("install JSON plugin");
    setup.execute(
        "INSERT INTO lix_file (path, content) VALUES ('/remote-conflict.json', $1)",
        &[Value::Blob(br#"{"value":"base"}"#.to_vec().into())],
    )
    .await
    .expect("write base JSON file");

    setup.close().await.expect("close setup Lix");
    let protocol = open_lix()
        .with_storage(storage.clone())
        .serve().with_embedded_lix_id()
        .await
        .expect("serve Lix");
    let sessions = [
        open_session(&protocol).await,
        open_session(&protocol).await,
        open_session(&protocol).await,
    ];
    let transactions = [
        begin_transaction(&protocol, &sessions[0]).await,
        begin_transaction(&protocol, &sessions[1]).await,
        begin_transaction(&protocol, &sessions[2]).await,
    ];

    for ((session, transaction), content) in sessions
        .iter()
        .zip(transactions.iter())
        .zip(["first", "second", "third"])
    {
        let encoded = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            format!(r#"{{"value":"{content}"}}"#),
        );
        let response = transaction_request(
            &protocol,
            "POST",
            "/lix/v1/transaction/execute",
            session,
            transaction,
            Some(json!({
                "sql": "UPDATE lix_file SET content = $1 WHERE path = '/remote-conflict.json'",
                "params": [{ "kind": "blob", "base64": encoded }]
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    let (first, second, third) = tokio::join!(
        commit(&protocol, &sessions[0], &transactions[0]),
        commit(&protocol, &sessions[1], &transactions[1]),
        commit(&protocol, &sessions[2], &transactions[2]),
    );
    for response in [first, second, third] {
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    let verifier = open_lix()
        .with_storage(storage)
        .await
        .expect("open verification Lix");
    let forks = verifier
        .execute(
            "SELECT parent_commit_ids ->> 0 AS parent_id, COUNT(*) AS children \
             FROM lix_commit WHERE parent_commit_ids ->> 0 IS NOT NULL \
             GROUP BY parent_commit_ids ->> 0 HAVING COUNT(*) > 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(forks.len(), 0, "remote writers must not fork history");
    verifier.close().await.expect("close verification Lix");

    let mut visible = Vec::new();
    for session in &sessions {
        let response = request(
            &protocol,
            "POST",
            "/lix/v1/execute",
            Some(session),
            None,
            Some(json!({
                "sql": "SELECT content FROM lix_file WHERE path = '/remote-conflict.json'"
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        visible.push(response_json(response).await["rows"].clone());
    }
    assert_eq!(visible[0], visible[1]);
    assert_eq!(visible[0], visible[2]);
    protocol.close().await.unwrap();
}

async fn open_session(protocol: &LixServerProtocol<Memory>) -> String {
    let response = request(protocol, "GET", "/lix/v1", None, None, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    response_json(response).await["sessionId"]
        .as_str()
        .expect("session ID")
        .to_owned()
}

async fn begin_transaction(protocol: &LixServerProtocol<Memory>, session: &str) -> String {
    let response = request(
        protocol,
        "POST",
        "/lix/v1/transaction/begin",
        Some(session),
        None,
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    response_json(response).await["transactionId"]
        .as_str()
        .expect("transaction ID")
        .to_owned()
}

async fn commit(
    protocol: &LixServerProtocol<Memory>,
    session: &str,
    transaction: &str,
) -> ServerProtocolResponse {
    transaction_request(
        protocol,
        "POST",
        "/lix/v1/transaction/commit",
        session,
        transaction,
        None,
    )
    .await
}

async fn transaction_request(
    protocol: &LixServerProtocol<Memory>,
    method: &str,
    path: &str,
    session: &str,
    transaction: &str,
    body: Option<JsonValue>,
) -> ServerProtocolResponse {
    request(
        protocol,
        method,
        path,
        Some(session),
        Some(transaction),
        body,
    )
    .await
}

async fn request(
    protocol: &LixServerProtocol<Memory>,
    method: &str,
    path: &str,
    session: Option<&str>,
    transaction: Option<&str>,
    body: Option<JsonValue>,
) -> ServerProtocolResponse {
    let suffix = path.strip_prefix("/lix/v1").expect("protocol test path");
    let targeted_path = format!("/lix/v1/{}{}", protocol.lix_id(), suffix);
    let mut builder = Request::builder().method(method).uri(targeted_path);
    if let Some(session) = session {
        builder = builder.header(SESSION_ID_HEADER, session);
    }
    if let Some(transaction) = transaction {
        builder = builder.header(TRANSACTION_ID_HEADER, transaction);
    }
    let body = if let Some(body) = body {
        builder = builder.header(CONTENT_TYPE, "application/json");
        ServerProtocolBody::from(body.to_string())
    } else {
        ServerProtocolBody::empty()
    };
    protocol
        .handle(
            builder.body(body).expect("protocol request"),
            ServerProtocolContext::anonymous(),
        )
        .await
}

async fn response_json(response: ServerProtocolResponse) -> JsonValue {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    serde_json::from_slice(&bytes).expect("JSON response")
}

fn json_plugin_archive() -> Vec<u8> {
    let wasm = std::fs::read(Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_JSON_plugin_json")))
        .expect("read JSON plugin component");
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/json/manifest.json").as_bytes(),
        ),
        (
            "schema/json_root.json",
            include_str!("../../../plugins/json/schema/json_root.json").as_bytes(),
        ),
        (
            "schema/json_object_member.json",
            include_str!("../../../plugins/json/schema/json_object_member.json").as_bytes(),
        ),
        (
            "schema/json_array_item.json",
            include_str!("../../../plugins/json/schema/json_array_item.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}
