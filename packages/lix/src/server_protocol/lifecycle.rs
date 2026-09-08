//! Hosted repository lifetime, independent of any storage backend or HTTP host.
use super::{
    SNAPSHOT_MEDIA_TYPE, ServerProtocolBody, ServerProtocolContext, ServerProtocolPrincipal,
    ServerProtocolRequest, ServerProtocolResponse,
};
use http::{Method, StatusCode, header};
use std::future::Future;

/// A host must authenticate and authorize before invoking this handler.
/// Creation stages and validates storage before atomically publishing it. Deletion
/// fences all sessions before acknowledging completion. Neither operation may
/// be abandoned merely because the HTTP caller disconnects.
pub trait LixLifecycleStore: Send + Sync {
    fn create(
        &self,
        scope: String,
        key: String,
        snapshot: Option<ServerProtocolBody>,
    ) -> impl Future<Output = Result<String, LifecycleError>> + Send;
    fn delete(&self, id: String) -> impl Future<Output = Result<(), LifecycleError>> + Send;
}

#[derive(Debug)]
pub struct LifecycleError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
}
impl LifecycleError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }
    fn response(self) -> ServerProtocolResponse {
        response(
            self.status,
            serde_json::json!({"error":{"code":self.code,"message":self.message}}),
        )
    }
}

/// Canonical lifecycle wire handler. `server_url` is host-controlled, never
/// taken from forwarding headers, and must identify the externally reachable host.
#[derive(Debug)]
pub struct LixServerLifecycle<Store> {
    store: Store,
    server_url: String,
}
impl<Store: LixLifecycleStore> LixServerLifecycle<Store> {
    pub fn new(store: Store, server_url: &str) -> Result<Self, LifecycleError> {
        let url = url::Url::parse(server_url).map_err(|_| invalid("Invalid server URL."))?;
        let loopback = match url.host() {
            Some(url::Host::Domain("localhost")) => true,
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };
        if !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
        {
            return Err(invalid(
                "Server URL must be an HTTPS origin (HTTP only for loopback), without credentials, path, query, or fragment.",
            ));
        }
        Ok(Self {
            store,
            server_url: url.to_string().trim_end_matches('/').to_owned(),
        })
    }
    /// Dispatch POST collection or DELETE repository. Hosts supply the parsed resource ID.
    pub async fn handle(
        &self,
        request: ServerProtocolRequest,
        id: Option<&str>,
        context: ServerProtocolContext,
    ) -> ServerProtocolResponse {
        match self.handle_inner(request, id, context).await {
            Ok(response) => response,
            Err(error) => error.response(),
        }
    }
    async fn handle_inner(
        &self,
        request: ServerProtocolRequest,
        id: Option<&str>,
        context: ServerProtocolContext,
    ) -> Result<ServerProtocolResponse, LifecycleError> {
        match (request.method(), id) {
            (&Method::POST, None) => {
                let key = request
                    .headers()
                    .get("idempotency-key")
                    .and_then(|v| v.to_str().ok())
                    .filter(|v| {
                        !v.is_empty() && v.len() <= 255 && v.bytes().all(|b| b.is_ascii_graphic())
                    })
                    .ok_or_else(|| {
                        invalid("A visible ASCII Idempotency-Key of 1 to 255 bytes is required.")
                    })?
                    .to_owned();
                if request.headers().get_all("idempotency-key").iter().count() != 1 {
                    return Err(invalid("Send Idempotency-Key once."));
                }
                let scope = match context.principal {
                    ServerProtocolPrincipal::Anonymous => "anonymous".to_owned(),
                    ServerProtocolPrincipal::Authenticated {
                        account_id,
                        idempotency_scope,
                    } => format!(
                        "account:{}:{account_id}{idempotency_scope}",
                        account_id.len()
                    ),
                };
                let snapshot = match request.headers().get(header::CONTENT_TYPE) {
                    Some(value) if value.to_str().ok() == Some(SNAPSHOT_MEDIA_TYPE) => {
                        Some(request.into_body())
                    }
                    Some(_) => {
                        return Err(LifecycleError::new(
                            StatusCode::UNSUPPORTED_MEDIA_TYPE,
                            "LIX_INVALID_ARGUMENT",
                            "Expected application/vnd.lix.snapshot.",
                        ));
                    }
                    None => {
                        request.into_body().into_bytes(0).await.map_err(|_| {
                            invalid("An empty create request must not contain a body.")
                        })?;
                        None
                    }
                };
                let id = self.store.create(scope, key, snapshot).await?;
                let url = format!("{}/lix/{id}", self.server_url);
                let mut result =
                    response(StatusCode::CREATED, serde_json::json!({"id":id,"url":url}));
                result.headers_mut().insert(
                    header::LOCATION,
                    url.parse()
                        .map_err(|_| invalid("Invalid repository URL."))?,
                );
                Ok(result)
            }
            (&Method::DELETE, Some(id)) => {
                if uuid::Uuid::parse_str(id)
                    .ok()
                    .map(|v| v.to_string())
                    .as_deref()
                    != Some(id)
                {
                    return Err(LifecycleError::new(
                        StatusCode::NOT_FOUND,
                        "LIX_NOT_FOUND",
                        "Lix not found.",
                    ));
                }
                request
                    .into_body()
                    .into_bytes(0)
                    .await
                    .map_err(|_| invalid("Delete must not contain a body."))?;
                self.store.delete(id.to_owned()).await?;
                Ok(http::Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .header(header::CACHE_CONTROL, "no-store")
                    .body(ServerProtocolBody::empty())
                    .expect("valid empty response"))
            }
            _ => Err(LifecycleError::new(
                StatusCode::METHOD_NOT_ALLOWED,
                "LIX_INVALID_ARGUMENT",
                "Unsupported lifecycle method.",
            )),
        }
    }
}
fn invalid(message: &str) -> LifecycleError {
    LifecycleError::new(StatusCode::BAD_REQUEST, "LIX_INVALID_ARGUMENT", message)
}
fn response(status: StatusCode, value: serde_json::Value) -> ServerProtocolResponse {
    http::Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(ServerProtocolBody::full(
            serde_json::to_vec(&value).expect("JSON response"),
        ))
        .expect("valid response")
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Store;
    impl LixLifecycleStore for Store {
        async fn create(
            &self,
            _: String,
            _: String,
            _: Option<ServerProtocolBody>,
        ) -> Result<String, LifecycleError> {
            Ok("11111111-1111-4111-8111-111111111111".to_owned())
        }
        async fn delete(&self, _: String) -> Result<(), LifecycleError> {
            Ok(())
        }
    }
    #[test]
    fn public_origin_requires_https_or_loopback_and_has_no_path() {
        for url in [
            "https://host.example",
            "http://localhost:8080",
            "http://127.0.0.2:8080",
            "http://[::1]:8080",
        ] {
            assert!(LixServerLifecycle::new(Store, url).is_ok(), "{url}");
        }
        for url in [
            "http://host.example",
            "https://host.example/lix/v1",
            "https://user:pass@host.example",
            "https://host.example?key=x",
            "https://host.example#fragment",
        ] {
            assert!(LixServerLifecycle::new(Store, url).is_err(), "{url}");
        }
    }
    #[tokio::test]
    async fn wire_requires_idempotency_and_returns_canonical_locator() {
        let handler = LixServerLifecycle::new(Store, "https://host.example").unwrap();
        let missing_key = http::Request::builder()
            .method("POST")
            .body(ServerProtocolBody::empty())
            .unwrap();
        assert_eq!(
            handler
                .handle(missing_key, None, ServerProtocolContext::anonymous())
                .await
                .status(),
            StatusCode::BAD_REQUEST
        );
        let create = http::Request::builder()
            .method("POST")
            .header("idempotency-key", "one")
            .body(ServerProtocolBody::empty())
            .unwrap();
        let response = handler
            .handle(create, None, ServerProtocolContext::anonymous())
            .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response.headers()[header::LOCATION],
            "https://host.example/lix/11111111-1111-4111-8111-111111111111"
        );
    }
}
