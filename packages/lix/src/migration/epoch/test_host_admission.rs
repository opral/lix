//! Minimal host-layer admission route for test-only raw protocol authorities.
//!
//! Production serves `/admission` in `packages/server`, outside the engine's
//! session protocol. Browser migration fixtures use `LixServerProtocol` over a
//! tiny local TCP bridge, so the bridge must model that one host-owned route.
use http::{HeaderMap, Method, Response, StatusCode, Uri, header};

pub(super) fn response(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    repository_id: &str,
) -> Option<Response<Vec<u8>>> {
    let path = uri.path();
    let rest = path.strip_prefix("/lix/v1/")?;
    let (requested_id, operation) = rest.split_once('/')?;
    if operation != "admission" {
        return None;
    }

    if requested_id != repository_id {
        return Some(error_response(
            StatusCode::NOT_FOUND,
            "LIX_NOT_FOUND",
            "Lix not found.",
        ));
    }
    if method != Method::GET {
        return Some(error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "LIX_INVALID_ARGUMENT",
            "Use GET for repository admission.",
        ));
    }
    if !exact_header(
        headers,
        crate::sync::SYNC_PROTOCOL_VERSION_HEADER,
        crate::SYNC_PROTOCOL_VERSION,
    ) {
        return Some(error_response(
            StatusCode::CONFLICT,
            "LIX_PROTOCOL_VERSION_MISMATCH",
            "Reload with the current Lix client before repository admission.",
        ));
    }

    let body = serde_json::to_vec(&serde_json::json!({
        "repositoryId": repository_id,
        "principalId": crate::ANONYMOUS_ACCOUNT_ID,
        "protocolEpoch": crate::SYNC_PROTOCOL_VERSION,
        "storageEpoch": crate::init::CURRENT_FORMAT_VERSION,
    }))
    .expect("fixture admission metadata serializes");
    Some(
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CACHE_CONTROL, "no-store")
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .expect("fixture admission response is valid"),
    )
}

fn exact_header(headers: &HeaderMap, name: &str, expected: u32) -> bool {
    let mut values = headers.get_all(name).iter();
    values.next().and_then(|value| value.to_str().ok()) == Some(expected.to_string().as_str())
        && values.next().is_none()
}

fn error_response(status: StatusCode, code: &str, message: &str) -> Response<Vec<u8>> {
    Response::builder()
        .status(status)
        .header(header::CACHE_CONTROL, "no-store")
        .header(header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::to_vec(&serde_json::json!({
                "error": { "code": code, "message": message }
            }))
            .expect("fixture admission error serializes"),
        )
        .expect("fixture admission error response is valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_protocol::{ServerProtocolBody, ServerProtocolContext};
    use http_body_util::BodyExt;

    fn version_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            crate::server_protocol::SERVER_PROTOCOL_VERSION_HEADER,
            crate::SERVER_PROTOCOL_VERSION.to_string().parse().unwrap(),
        );
        headers.insert(
            crate::sync::SYNC_PROTOCOL_VERSION_HEADER,
            crate::SYNC_PROTOCOL_VERSION.to_string().parse().unwrap(),
        );
        headers
    }

    #[test]
    fn admission_is_exact_repository_get_with_one_current_version_pair() {
        let id = "00000000-0000-7000-8000-000000000123";
        let uri: Uri = format!("/lix/v1/{id}/admission").parse().unwrap();
        let valid = response(&Method::GET, &uri, &version_headers(), id).unwrap();
        assert_eq!(valid.status(), StatusCode::OK);
        assert_eq!(valid.headers()[header::CACHE_CONTROL], "no-store");
        let metadata: serde_json::Value = serde_json::from_slice(valid.body()).unwrap();
        assert_eq!(metadata["repositoryId"], id);
        assert_eq!(metadata["principalId"], crate::ANONYMOUS_ACCOUNT_ID);
        assert_eq!(metadata["protocolEpoch"], crate::SYNC_PROTOCOL_VERSION);
        assert_eq!(
            metadata["storageEpoch"],
            crate::init::CURRENT_FORMAT_VERSION
        );
        let with_query: Uri = format!("/lix/v1/{id}/admission?source=browser")
            .parse()
            .unwrap();
        assert_eq!(
            response(&Method::GET, &with_query, &version_headers(), id)
                .unwrap()
                .status(),
            StatusCode::OK,
        );

        let unknown: Uri = "/lix/v1/00000000-0000-7000-8000-000000000124/admission"
            .parse()
            .unwrap();
        assert_eq!(
            response(&Method::GET, &unknown, &version_headers(), id)
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND,
        );
        assert_eq!(
            response(&Method::POST, &uri, &version_headers(), id)
                .unwrap()
                .status(),
            StatusCode::METHOD_NOT_ALLOWED,
        );

        let mut bad = version_headers();
        bad.insert(
            crate::sync::SYNC_PROTOCOL_VERSION_HEADER,
            (crate::SYNC_PROTOCOL_VERSION + 1)
                .to_string()
                .parse()
                .unwrap(),
        );
        assert_eq!(
            response(&Method::GET, &uri, &bad, id).unwrap().status(),
            StatusCode::CONFLICT,
        );

        let mut missing = version_headers();
        missing.remove(crate::sync::SYNC_PROTOCOL_VERSION_HEADER);
        assert_eq!(
            response(&Method::GET, &uri, &missing, id).unwrap().status(),
            StatusCode::CONFLICT,
        );

        let mut duplicate = version_headers();
        duplicate.append(
            crate::sync::SYNC_PROTOCOL_VERSION_HEADER,
            crate::SYNC_PROTOCOL_VERSION.to_string().parse().unwrap(),
        );
        assert_eq!(
            response(&Method::GET, &uri, &duplicate, id)
                .unwrap()
                .status(),
            StatusCode::CONFLICT,
        );
    }

    #[tokio::test]
    async fn unrelated_handshake_falls_through_to_the_real_protocol() {
        let protocol = crate::open_lix()
            .serve()
            .with_embedded_lix_id()
            .await
            .unwrap();
        let id = protocol.lix_id().to_owned();
        let uri: Uri = format!("/lix/v1/{id}/").parse().unwrap();
        let headers = version_headers();
        assert!(response(&Method::GET, &uri, &headers, &id).is_none());
        let mut request = http::Request::builder().method(Method::GET).uri(uri);
        *request.headers_mut().unwrap() = headers;
        let response = protocol
            .handle(
                request.body(ServerProtocolBody::empty()).unwrap(),
                ServerProtocolContext::anonymous(),
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let metadata: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            metadata["sessionId"]
                .as_str()
                .is_some_and(|id| !id.is_empty())
        );
        protocol.close().await.unwrap();
    }
}
