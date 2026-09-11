//! Repository sync HTTP protocol policy shared by native and browser clients.
//!
//! Target adapters implement only [`RawHttpClient`]: issuing a request,
//! cancellation, dynamic headers, and bounded response-body collection.

use http::Method;
use serde::Deserialize;

use super::{
    MAX_SYNC_PULL_RESPONSE_BYTES, SYNC_LONG_POLL_TIMEOUT, SYNC_PROTOCOL_VERSION,
    SYNC_PROTOCOL_VERSION_HEADER, SyncBlobManifest, SyncBlobRegistration, SyncHistoryResponse,
    SyncPushRequest, SyncPushResponse, SyncRepositoryPullResponse, SyncSnapshotRowPage,
    SyncTransport, SyncTransportBounds, SyncTransportFuture, sync_server_protocol_mismatch,
    sync_server_protocol_missing_field, validate_sync_remote_id,
};
use crate::LixError;

pub(super) const HTTP_TIMEOUT: std::time::Duration =
    SYNC_LONG_POLL_TIMEOUT.saturating_add(std::time::Duration::from_secs(5));
pub(super) const SYNC_TRANSPORT_ERROR_CODE: &str = "LIX_ERROR_SYNC_TRANSPORT";
const SESSION_HEADER: &str = "lix-session-id";

#[derive(Debug)]
pub(crate) struct RawHttpRequest {
    pub method: Method,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
    pub cache_immutable: bool,
    pub response_limit: usize,
    pub operation: &'static str,
}

#[derive(Debug)]
pub(crate) struct RawHttpResponse {
    pub status: u16,
    pub status_text: String,
    pub body: Vec<u8>,
}

pub(crate) trait RawHttpClient: SyncTransportBounds {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse>;
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HandshakeResponse {
    protocol_version: u32,
    #[serde(default)]
    sync_protocol_version: Option<u32>,
    lix_id: Option<String>,
    session_id: String,
    active_account_id: String,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    code: String,
    message: String,
    #[serde(default)]
    hint: Option<String>,
    #[serde(default)]
    details: Option<serde_json::Value>,
}

/// Fresh HTTP observation paired with a process-local publication deadline.
/// The deadline is deliberately absent from the wire and durable replica state.
#[derive(Debug)]
pub(crate) struct TimedLeasedPartialDescriptor {
    pub(crate) wire: super::LeasedPartialReplicaDescriptor,
    pub(crate) deadline: CandidateBaselineDeadline,
}

/// An HTTP-owned monotonic proof. Starting the budget before sending the
/// request conservatively charges network transfer and descriptor long-poll
/// time against the authority's fixed retention TTL, without clock-offset math.
#[derive(Clone, Debug)]
pub(crate) struct CandidateBaselineDeadline {
    lease_id: String,
    expires: web_time::Instant,
}
impl CandidateBaselineDeadline {
    fn from_request_start(lease_id: &str, started: web_time::Instant) -> Self {
        Self {
            lease_id: lease_id.to_owned(),
            expires: started
                + std::time::Duration::from_millis(crate::gc::NATIVE_BASELINE_LEASE_TTL_MS),
        }
    }
    pub(crate) fn check(&self, lease_id: &str) -> Result<(), LixError> {
        if self.lease_id != lease_id {
            return Err(Self::expired(
                "candidate deadline belongs to another baseline lease",
            ));
        }
        self.remaining().map(|_| ())
    }
    pub(crate) fn remaining(&self) -> Result<std::time::Duration, LixError> {
        self.expires
            .checked_duration_since(web_time::Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| {
                Self::expired(
                    "candidate baseline deadline elapsed; prepare a fresh leased candidate",
                )
            })
    }
    fn expired(message: &str) -> LixError {
        LixError::new("LIX_PARTIAL_CANDIDATE_EXPIRED", message)
    }
    #[cfg(test)]
    pub(crate) fn for_test(lease_id: &str, duration: std::time::Duration) -> Self {
        Self {
            lease_id: lease_id.to_owned(),
            expires: web_time::Instant::now() + duration,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct HttpSyncTransport<Client> {
    client: Client,
    protocol_url: String,
    lix_id: String,
    session_id: String,
    active_account_id: String,
    baseline_lease: std::sync::Arc<parking_lot::Mutex<Option<crate::gc::NativeBaselineLease>>>,
}

impl<Client> HttpSyncTransport<Client>
where
    Client: RawHttpClient,
{
    pub(super) async fn connect_with(client: Client, lix_url: &str) -> Result<Self, LixError> {
        let normalized = normalize_sync_locator(lix_url)?;
        let protocol_url = normalized.protocol_url;
        let response = client
            .send(raw_request(
                Method::GET,
                protocol_url.clone(),
                "open sync session",
            ))
            .await?;
        let handshake: HandshakeResponse = decode_response(response, "open sync session")?;
        let lix_id = validate_handshake(&handshake)?.to_owned();
        Ok(Self {
            client,
            protocol_url,
            lix_id,
            session_id: handshake.session_id,
            active_account_id: handshake.active_account_id,
            baseline_lease: Default::default(),
        })
    }

    // Insert in inherent HttpSyncTransport<Client:RawHttpClient> impl.
    pub(crate) fn restart_partial_attempt<'a>(
        &'a self,
        value: &'a super::PartialAttemptRestartRequest,
    ) -> SyncTransportFuture<'a, super::PartialAttemptRestartOutcome> {
        Box::pin(async move {
            value.validate()?;
            let mut request = self.request(
                Method::POST,
                "/sync/merge/restart",
                "restart expired partial merge",
            );
            request.response_limit = 4096;
            request.headers.push(json_content_type());
            request.body = Some(json_body(value, "encode partial merge restart")?);
            let response = self.client.send(request).await?;
            if response.body.len() > 4096 {
                return Err(response_too_large_limit(
                    "restart expired partial merge",
                    4096,
                ));
            }
            let outcome: super::PartialAttemptRestartOutcome =
                decode_response(response, "restart expired partial merge")?;
            outcome.validate_for(&self.lix_id, &self.active_account_id, value)?;
            Ok(outcome)
        })
    }

    pub(crate) fn retained_body_wave<'a>(
        &'a self,
        value: &'a super::RetainedBodyWaveRequest,
    ) -> SyncTransportFuture<'a, super::RetainedBodyWaveResponse> {
        Box::pin(async move {
            value.validate()?;
            let tip = &value
                .bodies
                .commits
                .last()
                .expect("validated nonempty wave")
                .commit_id;
            let mut request = self.request(
                Method::POST,
                "/sync/retained-bodies",
                "retain native upload wave",
            );
            request.response_limit = 2048;
            request.headers.push(json_content_type());
            request.body = Some(json_body(value, "encode retained body wave")?);
            let response = self.client.send(request).await?;
            if response.body.len() > 2048 {
                return Err(response_too_large_limit("retain native upload wave", 2048));
            }
            let result: super::RetainedBodyWaveResponse =
                decode_response(response, "retain native upload wave")?;
            if &result.accepted_tip != tip || result.expires_at_ms == 0 {
                return Err(LixError::new(
                    super::SYNC_PROTOCOL_MISMATCH_CODE,
                    "retained upload acknowledgment changed the captured tip or omitted expiry",
                ));
            }
            Ok(result)
        })
    }
    pub(crate) fn cleanup_native_migration<'a>(
        &'a self,
        value: &'a super::NativeMigrationCleanupRequest,
    ) -> SyncTransportFuture<'a, SyncPushResponse> {
        Box::pin(async move {
            value.validate()?;
            let mut request = self.request(
                Method::POST,
                "/sync/migration/cleanup",
                "merge partial replica commits",
            );
            request.response_limit = 4096;
            request.headers.push(json_content_type());
            request.body = Some(json_body(value, "encode partial merge")?);
            let response = self.client.send(request).await?;
            if response.body.len() > 4096 {
                return Err(response_too_large_limit(
                    "merge partial replica commits",
                    4096,
                ));
            }
            let result: SyncPushResponse =
                decode_response(response, "merge partial replica commits")?;
            Ok(result)
        })
    }
    pub(crate) fn restart_native_global_migration<'a>(
        &'a self,
        value: &'a super::NativeGlobalRestartRequest,
    ) -> SyncTransportFuture<'a, super::NativeGlobalRestartReceipt> {
        Box::pin(async move {
            value.validate()?;
            let mut request = self.request(
                Method::POST,
                "/sync/migration/global/restart",
                "restart global migration",
            );
            request.response_limit = 256 * 1024;
            request.headers.push(json_content_type());
            request.body = Some(json_body(value, "encode global restart")?);
            let response = self.client.send(request).await?;
            if response.body.len() > 256 * 1024 {
                return Err(response_too_large_limit("global restart", 256 * 1024));
            }
            let result: super::NativeGlobalRestartReceipt =
                decode_response(response, "global restart")?;
            result.validate_for(value)?;
            Ok(result)
        })
    }
    pub(crate) fn cleanup_native_global_migration<'a>(
        &'a self,
        value: &'a super::NativeGlobalMigrationRequest,
    ) -> SyncTransportFuture<'a, bool> {
        Box::pin(async move {
            value.validate()?;
            let mut request = self.request(
                Method::POST,
                "/sync/migration/global/cleanup",
                "cleanup global migration",
            );
            request.response_limit = 4096;
            request.headers.push(json_content_type());
            request.body = Some(json_body(value, "encode global cleanup")?);
            let response = self.client.send(request).await?;
            if response.body.len() > 4096 {
                return Err(response_too_large_limit("global cleanup", 4096));
            }
            decode_response(response, "global cleanup")
        })
    }
    pub(crate) fn merge_native_global_migration<'a>(
        &'a self,
        value: &'a super::NativeGlobalMigrationRequest,
    ) -> SyncTransportFuture<'a, super::NativeGlobalMigrationReceipt> {
        Box::pin(async move {
            value.validate()?;
            let mut request = self.request(
                Method::POST,
                "/sync/migration/global/merge",
                "merge migration global descriptors",
            );
            request.response_limit = 256 * 1024;
            request.headers.push(json_content_type());
            request.body = Some(json_body(value, "encode global migration")?);
            let response = self.client.send(request).await?;
            if response.body.len() > 256 * 1024 {
                return Err(response_too_large_limit(
                    "global migration outcome",
                    256 * 1024,
                ));
            }
            let result: super::NativeGlobalMigrationReceipt =
                decode_response(response, "global migration outcome")?;
            result.validate()?;
            if &result.request != value {
                return Err(LixError::new(
                    "LIX_MIGRATION_GLOBAL_BODY_INVALID",
                    "authority global outcome does not match exact request",
                ));
            }
            Ok(result)
        })
    }
    pub(crate) fn push_native_global_migration_wave<'a>(
        &'a self,
        value: &'a super::NativeGlobalBodyWaveRequest,
    ) -> SyncTransportFuture<'a, SyncPushResponse> {
        Box::pin(async move {
            value.validate()?;
            let mut request = self.request(
                Method::POST,
                "/sync/migration/global/bodies",
                "retain native global migration bodies",
            );
            request.response_limit = 4096;
            request.headers.push(json_content_type());
            request.body = Some(json_body(value, "encode migration body wave")?);
            let response = self.client.send(request).await?;
            if response.body.len() > 4096 {
                return Err(response_too_large_limit(
                    "migration body acknowledgement",
                    4096,
                ));
            }
            decode_response(response, "migration body acknowledgement")
        })
    }

    pub(crate) fn merge_native_migration<'a>(
        &'a self,
        value: &'a super::NativeMigrationMergeRequest,
    ) -> SyncTransportFuture<'a, super::PartialMergeReceipt> {
        Box::pin(async move {
            value.validate()?;
            let mut request = self.request(
                Method::POST,
                "/sync/migration/merge",
                "merge partial replica commits",
            );
            request.response_limit = 4096;
            request.headers.push(json_content_type());
            request.body = Some(json_body(value, "encode partial merge")?);
            let response = self.client.send(request).await?;
            if response.body.len() > 4096 {
                return Err(response_too_large_limit(
                    "merge partial replica commits",
                    4096,
                ));
            }
            let result: super::PartialMergeReceipt =
                decode_response(response, "merge partial replica commits")?;
            result.validate_for(&value.request)?;
            Ok(result)
        })
    }
    pub(crate) fn merge_partial_replica<'a>(
        &'a self,
        value: &'a super::PartialMergeRequest,
    ) -> SyncTransportFuture<'a, super::PartialMergeReceipt> {
        Box::pin(async move {
            value.validate()?;
            let mut request =
                self.request(Method::POST, "/sync/merge", "merge partial replica commits");
            request.response_limit = 4096;
            request.headers.push(json_content_type());
            request.body = Some(json_body(value, "encode partial merge")?);
            let response = self.client.send(request).await?;
            if response.body.len() > 4096 {
                return Err(response_too_large_limit(
                    "merge partial replica commits",
                    4096,
                ));
            }
            let result: super::PartialMergeReceipt =
                decode_response(response, "merge partial replica commits")?;
            result.validate_for(value)?;
            Ok(result)
        })
    }
    pub(crate) fn bind_native_baseline_lease(
        &self,
        lease: &crate::gc::NativeBaselineLease,
    ) -> Result<(), LixError> {
        lease.validate()?;
        if lease.account_id != self.active_account_id {
            return Err(LixError::new(
                super::SYNC_PROTOCOL_MISMATCH_CODE,
                "baseline lease account mismatch",
            ));
        }
        let mut bound = self.baseline_lease.lock();
        if bound.as_ref().is_some_and(|prior| {
            let mut expected = prior.clone();
            expected.expires_at_ms = lease.expires_at_ms;
            &expected != lease
        }) {
            return Err(LixError::new(
                super::SYNC_PROTOCOL_MISMATCH_CODE,
                "transport already bound to another baseline lease",
            ));
        }
        *bound = Some(lease.clone());
        Ok(())
    }
    pub(crate) fn fork_native_baseline_lease(
        &self,
        lease: &crate::gc::NativeBaselineLease,
    ) -> Result<Self, LixError>
    where
        Client: Clone,
    {
        let mut fork = self.clone();
        fork.baseline_lease = Default::default();
        fork.bind_native_baseline_lease(lease)?;
        Ok(fork)
    }

    pub(crate) async fn renew_native_baseline_lease(
        &self,
    ) -> Result<crate::gc::NativeBaselineLease, LixError> {
        self.renew_native_baseline_lease_timed()
            .await
            .map(|(lease, _)| lease)
    }
    pub(crate) async fn renew_native_baseline_lease_timed(
        &self,
    ) -> Result<(crate::gc::NativeBaselineLease, CandidateBaselineDeadline), LixError> {
        let prior = self.baseline_lease.lock().clone().ok_or_else(|| {
            LixError::new(
                super::SYNC_PROTOCOL_MISMATCH_CODE,
                "renewal requires a bound baseline lease",
            )
        })?;
        let mut request = self.request(
            Method::POST,
            "/sync/baseline-lease/renew",
            "renew native baseline lease",
        );
        request.response_limit = 1024;
        request.headers.push(json_content_type());
        request.body = Some(json_body(
            &serde_json::json!({"leaseId":prior.lease_id}),
            "encode baseline renewal",
        )?);
        let request_started = web_time::Instant::now();
        let response = self.client.send(request).await?;
        if response.body.len() > 1024 {
            return Err(response_too_large_limit(
                "renew native baseline lease",
                1024,
            ));
        }
        let renewed: crate::gc::NativeBaselineLease =
            decode_response(response, "renew native baseline lease")?;
        let mut expected = prior.clone();
        expected.expires_at_ms = renewed.expires_at_ms;
        if renewed != expected || renewed.expires_at_ms < prior.expires_at_ms {
            return Err(LixError::new(
                super::SYNC_PROTOCOL_MISMATCH_CODE,
                "baseline renewal changed its immutable binding",
            ));
        }
        let deadline =
            CandidateBaselineDeadline::from_request_start(&renewed.lease_id, request_started);
        deadline.check(&renewed.lease_id)?;
        *self.baseline_lease.lock() = Some(renewed.clone());
        Ok((renewed, deadline))
    }

    fn require_bound_native_lease(&self) -> Result<(), LixError> {
        if self.baseline_lease.lock().is_none() {
            return Err(LixError::new(
                super::SYNC_PROTOCOL_MISMATCH_CODE,
                "native reads require an admitted baseline lease",
            ));
        }
        Ok(())
    }

    pub(crate) async fn close_session(&self) -> Result<(), LixError> {
        let response = self
            .client
            .send(self.request(Method::DELETE, "/session", "close sync session"))
            .await?;
        ensure_success(&response, "close sync session")
    }

    pub(super) fn is_reserved_header(name: &str) -> bool {
        name.eq_ignore_ascii_case(SESSION_HEADER)
            || name.eq_ignore_ascii_case(SYNC_PROTOCOL_VERSION_HEADER)
            || name.eq_ignore_ascii_case("lix-server-protocol-version")
    }

    pub(super) fn protocol_url(&self) -> &str {
        &self.protocol_url
    }

    pub(super) fn lix_id(&self) -> &str {
        &self.lix_id
    }

    /// Fetches coordinates without activating a partial replica or changing
    /// full-sync bootstrap/certification state.
    pub(crate) fn partial_replica_descriptor<'a>(
        &'a self,
        branch_id: Option<&'a str>,
    ) -> SyncTransportFuture<'a, TimedLeasedPartialDescriptor> {
        self.fetch_partial_replica_descriptor(branch_id, None)
    }

    /// Waits for a newer coherent descriptor, or returns the current one after
    /// the fixed server deadline. The caller reconciles roots before publishing
    /// its cursor; receiving a descriptor does not change local coverage.
    pub(crate) fn wait_partial_replica_descriptor<'a>(
        &'a self,
        branch_id: &'a str,
        after: u64,
    ) -> SyncTransportFuture<'a, TimedLeasedPartialDescriptor> {
        self.fetch_partial_replica_descriptor(Some(branch_id), Some(after))
    }

    fn fetch_partial_replica_descriptor<'a>(
        &'a self,
        branch_id: Option<&'a str>,
        after: Option<u64>,
    ) -> SyncTransportFuture<'a, TimedLeasedPartialDescriptor> {
        Box::pin(async move {
            if let Some(branch_id) = branch_id {
                super::validate_sync_branch_id(branch_id)?;
            }
            let mut path = match branch_id {
                Some(branch_id) => format!("/sync/descriptor?branchId={}", encode_query(branch_id)),
                None => "/sync/descriptor".to_owned(),
            };
            if let Some(after) = after {
                path.push_str(&format!("&after={after}"));
            }
            let mut request = self.request(Method::GET, &path, "load partial replica descriptor");
            request.response_limit = super::MAX_LEASED_DESCRIPTOR_BYTES;
            let request_started = web_time::Instant::now();
            let response = self.client.send(request).await?;
            if response.body.len() > super::MAX_LEASED_DESCRIPTOR_BYTES {
                return Err(LixError::new(
                    super::SYNC_PROTOCOL_MISMATCH_CODE,
                    "partial replica descriptor exceeds its byte limit",
                ));
            }
            let descriptor: super::LeasedPartialReplicaDescriptor =
                decode_response(response, "load partial replica descriptor")?;
            descriptor.validate(&self.lix_id, &self.active_account_id, branch_id)?;
            if after.is_some_and(|after| descriptor.descriptor.cursor < after) {
                return Err(LixError::new(
                    super::SYNC_PROTOCOL_MISMATCH_CODE,
                    "partial replica descriptor regressed behind the requested cursor",
                ));
            }
            let deadline = CandidateBaselineDeadline::from_request_start(
                &descriptor.lease.lease_id,
                request_started,
            );
            deadline.check(&descriptor.lease.lease_id)?;
            Ok(TimedLeasedPartialDescriptor {
                wire: descriptor,
                deadline,
            })
        })
    }

    /// Fetches only explicitly typed, content-addressed native objects.
    /// Validation does not publish objects, install refs, or certify coverage.
    pub(crate) fn native_objects<'a>(
        &'a self,
        objects: &'a [crate::tracked_state::NativeObjectRef],
    ) -> SyncTransportFuture<'a, super::native_object::NativeObjectResponse> {
        Box::pin(async move {
            super::native_object::validate_request(objects)?;
            self.require_bound_native_lease()?;
            let mut request =
                self.request(Method::POST, "/sync/native-objects", "load native objects");
            request.response_limit = super::native_object::MAX_NATIVE_OBJECT_RESPONSE_BYTES;
            request.headers.push(json_content_type());
            request.body = Some(json_body(
                &serde_json::json!({"objects": objects}),
                "encode native object request",
            )?);
            let response = self.client.send(request).await?;
            if response.body.len() > super::native_object::MAX_NATIVE_OBJECT_RESPONSE_BYTES {
                return Err(response_too_large_limit(
                    "load native objects",
                    super::native_object::MAX_NATIVE_OBJECT_RESPONSE_BYTES,
                ));
            }
            let response: super::native_object::NativeObjectResponse =
                decode_response(response, "load native objects")?;
            super::native_object::validate_response(&self.lix_id, objects, &response)?;
            Ok(response)
        })
    }

    /// Fetches a bounded byte range. Range bytes are not hash-verified on
    /// their own and must pass NativeObjectAssembler before native publication.
    pub(crate) fn native_object_range<'a>(
        &'a self,
        range: &'a super::native_object_range::NativeObjectRangeRequest,
    ) -> SyncTransportFuture<'a, super::native_object_range::NativeObjectRangeResponse> {
        Box::pin(async move {
            super::native_object_range::validate_range_request(range)?;
            self.require_bound_native_lease()?;
            let mut request = self.request(
                Method::POST,
                "/sync/native-object-range",
                "load native object range",
            );
            request.response_limit = super::native_object::MAX_NATIVE_OBJECT_RESPONSE_BYTES;
            request.headers.push(json_content_type());
            request.body = Some(json_body(range, "encode native object range request")?);
            let response = self.client.send(request).await?;
            if response.body.len() > super::native_object::MAX_NATIVE_OBJECT_RESPONSE_BYTES {
                return Err(response_too_large_limit(
                    "load native object range",
                    super::native_object::MAX_NATIVE_OBJECT_RESPONSE_BYTES,
                ));
            }
            let response: super::native_object_range::NativeObjectRangeResponse =
                decode_response(response, "load native object range")?;
            let remaining = response.total_bytes.checked_sub(range.offset);
            if response.lix_id != self.lix_id
                || response.address != range.address
                || response.offset != range.offset
                || remaining.is_none_or(|remaining| {
                    response.bytes.len() as u64 != remaining.min(u64::from(range.max_bytes))
                })
            {
                return Err(LixError::new(
                    super::SYNC_PROTOCOL_MISMATCH_CODE,
                    "native object range response disagrees with the request",
                ));
            }
            Ok(response)
        })
    }

    /// Fetches native authority records; the epoch correlates the caller's
    /// durable receipt and does not grant a server retention lease.
    pub(crate) fn native_metadata<'a>(
        &'a self,
        metadata: &'a super::native_metadata::NativeMetadataRequest,
    ) -> SyncTransportFuture<'a, super::native_metadata::NativeMetadataResponse> {
        Box::pin(async move {
            super::native_metadata::validate_native_metadata_request(metadata)?;
            self.require_bound_native_lease()?;
            let mut request = self.request(
                Method::POST,
                "/sync/native-metadata",
                "load native metadata",
            );
            request.response_limit = super::native_metadata::MAX_NATIVE_METADATA_RESPONSE_BYTES;
            request.headers.push(json_content_type());
            request.body = Some(json_body(metadata, "encode native metadata request")?);
            let response = self.client.send(request).await?;
            if response.body.len() > super::native_metadata::MAX_NATIVE_METADATA_RESPONSE_BYTES {
                return Err(response_too_large_limit(
                    "load native metadata",
                    super::native_metadata::MAX_NATIVE_METADATA_RESPONSE_BYTES,
                ));
            }
            let response = decode_response(response, "load native metadata")?;
            super::native_metadata::validate_native_metadata_response(
                &self.lix_id,
                metadata,
                &response,
            )?;
            Ok(response)
        })
    }

    fn request(&self, method: Method, path: &str, operation: &'static str) -> RawHttpRequest {
        let mut request = raw_request(method, format!("{}{path}", self.protocol_url), operation);
        request
            .headers
            .push((SESSION_HEADER.to_owned(), self.session_id.clone()));
        request.headers.push((
            SYNC_PROTOCOL_VERSION_HEADER.to_owned(),
            SYNC_PROTOCOL_VERSION.to_string(),
        ));
        if let Some(lease) = self.baseline_lease.lock().as_ref() {
            request
                .headers
                .push(("lix-native-baseline-lease".into(), lease.lease_id.clone()));
        }
        request
    }

    async fn send_json<T>(&self, request: RawHttpRequest) -> Result<T, LixError>
    where
        T: serde::de::DeserializeOwned,
    {
        let operation = request.operation;
        decode_response(self.client.send(request).await?, operation)
    }
}

fn validate_handshake(handshake: &HandshakeResponse) -> Result<&str, LixError> {
    if handshake.protocol_version != crate::SERVER_PROTOCOL_VERSION {
        return Err(LixError::new(
            "LIX_SERVER_PROTOCOL_ERROR",
            format!(
                "unsupported Lix Server Protocol version: {}",
                handshake.protocol_version
            ),
        ));
    }
    if handshake.sync_protocol_version != Some(SYNC_PROTOCOL_VERSION) {
        return Err(sync_server_protocol_mismatch(
            handshake.sync_protocol_version,
        ));
    }
    let lix_id = handshake
        .lix_id
        .as_deref()
        .ok_or_else(|| sync_server_protocol_missing_field("lixId"))?;
    crate::row_pk::RowPk::uuid_from_canonical(lix_id).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync handshake returned an invalid lixId",
        )
    })?;
    if handshake.session_id.is_empty() || handshake.session_id.len() > 4096 {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync handshake returned an invalid session identity",
        ));
    }
    crate::row_pk::RowPk::uuid_from_canonical(&handshake.active_account_id).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync handshake returned an invalid active account identity",
        )
    })?;
    Ok(lix_id)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NormalizedSyncLocator {
    pub(crate) locator: String,
    pub(crate) protocol_url: String,
}

pub(crate) fn normalize_sync_locator(locator: &str) -> Result<NormalizedSyncLocator, LixError> {
    let mut parsed = url::Url::parse(locator).map_err(|_| invalid_lix_locator())?;
    if parsed.scheme() != "https" && !(parsed.scheme() == "http" && is_loopback_host(&parsed)) {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync server url must use https (http is allowed only for loopback development)",
        ));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync server url must not contain a query or fragment",
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync server url must not contain credentials",
        ));
    }
    let locator_path = parsed.path();
    let Some(lix_id) = locator_path.strip_prefix("/lix/") else {
        return Err(invalid_lix_locator());
    };
    if lix_id.contains('/') || crate::row_pk::RowPk::uuid_from_canonical(lix_id).is_err() {
        return Err(invalid_lix_locator());
    }
    let lix_id = lix_id.to_owned();
    parsed.set_path(&format!("/lix/{lix_id}"));
    let canonical_locator = parsed.to_string();
    validate_sync_remote_id(&canonical_locator)?;
    parsed.set_path(&format!("/lix/v1/{lix_id}"));
    Ok(NormalizedSyncLocator {
        locator: canonical_locator,
        protocol_url: parsed.to_string(),
    })
}

fn is_loopback_host(url: &url::Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

fn invalid_lix_locator() -> LixError {
    LixError::new(
        LixError::CODE_INVALID_PARAM,
        "sync server url path must be exactly /lix/{uuid}",
    )
}

impl<Client> SyncTransport for HttpSyncTransport<Client>
where
    Client: RawHttpClient,
{
    fn active_account_id(&self) -> &str {
        &self.active_account_id
    }

    fn push<'a>(&'a self, value: &'a SyncPushRequest) -> SyncTransportFuture<'a, SyncPushResponse> {
        Box::pin(async move {
            let mut request = self.request(Method::POST, "/sync/push", "push sync commits");
            request.headers.push(json_content_type());
            request.body = Some(json_body(value, "encode sync push")?);
            self.send_json(request).await
        })
    }

    fn pull(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> SyncTransportFuture<'_, SyncRepositoryPullResponse> {
        Box::pin(async move {
            let path = match after {
                Some(after) => format!("/sync/pull?after={after}&limit={limit}"),
                None => format!("/sync/pull?limit={limit}"),
            };
            let request = self.request(Method::GET, &path, "pull sync repository");
            self.send_json(request).await
        })
    }

    fn pull_now(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> SyncTransportFuture<'_, SyncRepositoryPullResponse> {
        Box::pin(async move {
            let path = match after {
                Some(after) => format!("/sync/pull?after={after}&limit={limit}"),
                None => format!("/sync/pull?limit={limit}"),
            };
            let mut request = self.request(Method::GET, &path, "fence sync publication");
            request
                .headers
                .push(("prefer".to_owned(), "wait=0".to_owned()));
            self.send_json(request).await
        })
    }

    fn snapshot_rows<'a>(
        &'a self,
        branch_id: &'a str,
        head_commit_id: &'a str,
        continuation: Option<&'a str>,
        limit: usize,
    ) -> SyncTransportFuture<'a, SyncSnapshotRowPage> {
        Box::pin(async move {
            let mut path = format!(
                "/sync/pull?snapshotBranchId={}&snapshotHeadCommitId={}&limit={limit}",
                encode_query(branch_id),
                encode_query(head_commit_id),
            );
            if let Some(continuation) = continuation {
                path.push_str("&snapshotAfter=");
                path.push_str(&encode_query(continuation));
            }
            let request = self.request(Method::GET, &path, "load sync snapshot rows");
            self.send_json(request).await
        })
    }

    fn checkpoint_inventory<'a>(
        &'a self,
        cursor: u64,
        after: Option<&'a str>,
        limit: usize,
    ) -> SyncTransportFuture<'a, super::SyncCheckpointInventoryPage> {
        Box::pin(async move {
            let mut path = format!("/sync/checkpoints?cursor={cursor}&limit={limit}");
            if let Some(after) = after {
                path.push_str("&after=");
                path.push_str(&encode_query(after));
            }
            self.send_json(self.request(Method::GET, &path, "load checkpoint inventory"))
                .await
        })
    }

    fn history<'a>(
        &'a self,
        head: &'a str,
        limit: usize,
    ) -> SyncTransportFuture<'a, SyncHistoryResponse> {
        Box::pin(async move {
            let query = format!("head={}&limit={limit}", encode_query(head));
            let request = self.request(
                Method::GET,
                &format!("/sync/history?{query}"),
                "load sync history",
            );
            self.send_json(request).await
        })
    }

    fn get_blobs<'a>(
        &'a self,
        blob_ids: &'a [String],
    ) -> SyncTransportFuture<'a, Vec<SyncBlobManifest>> {
        Box::pin(async move {
            let blob_ids = blob_ids
                .iter()
                .map(|blob_id| encode_query(blob_id))
                .collect::<Vec<_>>()
                .join(",");
            let request = self.request(
                Method::GET,
                &format!("/sync/blob?blobIds={blob_ids}"),
                "load sync blob manifests",
            );
            self.send_json(request).await
        })
    }

    fn register_blob<'a>(
        &'a self,
        manifest: &'a SyncBlobManifest,
    ) -> SyncTransportFuture<'a, SyncBlobRegistration> {
        Box::pin(async move {
            let mut request =
                self.request(Method::POST, "/sync/blob", "register sync blob manifest");
            request.headers.push(json_content_type());
            request.body = Some(json_body(manifest, "encode sync blob manifest")?);
            self.send_json(request).await
        })
    }

    fn get_chunk<'a>(&'a self, chunk_id: &'a str) -> SyncTransportFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let mut request = self.request(
                Method::GET,
                &format!("/sync/chunk?chunkId={}", encode_query(chunk_id)),
                "load sync chunk",
            );
            request.cache_immutable = true;
            request.response_limit = 4 * 1024 * 1024;
            let response = self.client.send(request).await?;
            if response.status == 404 {
                return Ok(None);
            }
            ensure_success(&response, "load sync chunk")?;
            Ok(Some(response.body))
        })
    }

    fn put_chunk<'a>(&'a self, chunk_id: &'a str, bytes: &'a [u8]) -> SyncTransportFuture<'a, ()> {
        Box::pin(async move {
            let mut request = self.request(Method::PUT, "/sync/chunk", "store sync chunk");
            request.url.push_str("?chunkId=");
            request.url.push_str(&encode_query(chunk_id));
            request.headers.push((
                "content-type".to_owned(),
                "application/octet-stream".to_owned(),
            ));
            request.body = Some(bytes.to_vec());
            let response = self.client.send(request).await?;
            ensure_success(&response, "store sync chunk")
        })
    }
}

fn raw_request(method: Method, url: String, operation: &'static str) -> RawHttpRequest {
    RawHttpRequest {
        method,
        url,
        headers: vec![(
            "lix-server-protocol-version".to_owned(),
            crate::SERVER_PROTOCOL_VERSION.to_string(),
        )],
        body: None,
        cache_immutable: false,
        response_limit: MAX_SYNC_PULL_RESPONSE_BYTES,
        operation,
    }
}

#[allow(
    dead_code,
    reason = "retained with the authority write operations on the complete sync transport"
)]
fn json_content_type() -> (String, String) {
    ("content-type".to_owned(), "application/json".to_owned())
}

#[allow(
    dead_code,
    reason = "retained with the authority write operations on the complete sync transport"
)]
fn json_body(value: &impl serde::Serialize, operation: &str) -> Result<Vec<u8>, LixError> {
    serde_json::to_vec(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("{operation}: {error}"),
        )
    })
}

fn decode_response<T>(response: RawHttpResponse, operation: &str) -> Result<T, LixError>
where
    T: serde::de::DeserializeOwned,
{
    ensure_success(&response, operation)?;
    serde_json::from_slice(&response.body).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("decode {operation} response: {error}"),
        )
    })
}

fn ensure_success(response: &RawHttpResponse, operation: &str) -> Result<(), LixError> {
    if response.body.len() > MAX_SYNC_PULL_RESPONSE_BYTES {
        return Err(response_too_large(operation));
    }
    if (200..300).contains(&response.status) {
        return Ok(());
    }
    Err(response_error(response, operation))
}

pub(super) fn response_too_large(operation: &str) -> LixError {
    response_too_large_limit(operation, MAX_SYNC_PULL_RESPONSE_BYTES)
}

pub(super) fn response_too_large_limit(operation: &str, limit: usize) -> LixError {
    LixError::new(
        LixError::CODE_INVALID_PARAM,
        format!("{operation} response exceeds {limit} bytes"),
    )
}

fn response_error(response: &RawHttpResponse, operation: &str) -> LixError {
    if let Ok(envelope) = serde_json::from_slice::<ErrorResponse>(&response.body)
        && !envelope.error.code.is_empty()
    {
        let mut details = envelope
            .error
            .details
            .unwrap_or_else(|| serde_json::json!({}));
        if let Some(object) = details.as_object_mut() {
            object.insert("httpStatus".to_owned(), serde_json::json!(response.status));
        } else {
            details = serde_json::json!({
                "httpStatus": response.status,
                "body": details,
            });
        }
        let mut error = LixError::new(
            envelope.error.code,
            format!("{operation}: {}", envelope.error.message),
        );
        if let Some(hint) = envelope.error.hint {
            error = error.with_hint(hint);
        }
        return error.with_details(details);
    }
    if response.status == 413 {
        return LixError::new(
            "LIX_ERROR_REQUEST_BODY_TOO_LARGE",
            format!("{operation} exceeded an HTTP intermediary transfer limit"),
        );
    }
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "{operation} failed with {} {}: {}",
            response.status,
            response.status_text,
            String::from_utf8_lossy(&response.body)
        ),
    )
    .with_details(serde_json::json!({ "httpStatus": response.status }))
}

fn encode_query(value: &str) -> String {
    use std::fmt::Write;

    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::{
        HandshakeResponse, HttpSyncTransport, RawHttpClient, RawHttpRequest, RawHttpResponse,
        encode_query, normalize_sync_locator, response_error, validate_handshake,
    };
    use crate::sync::{SyncTransport, SyncTransportFuture};

    #[test]
    fn sync_connection_locator_maps_to_the_targeted_protocol_root() {
        assert_eq!(
            normalize_sync_locator("https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc")
                .expect("canonical locator")
                .protocol_url,
            "https://example.test/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc"
        );
        assert_eq!(
            normalize_sync_locator(
                "https://EXAMPLE.test:443/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc"
            )
            .expect("equivalent locator")
            .locator,
            "https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc"
        );
    }

    #[test]
    fn sync_connection_locator_rejects_non_http_and_credentialed_urls() {
        for invalid in [
            "relative/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
            "ftp://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
            "http://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
            "https://example.test/prefix/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
            "https://user@example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
            "https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc/",
        ] {
            assert!(
                normalize_sync_locator(invalid).is_err(),
                "accepted invalid locator: {invalid}"
            );
        }
        assert!(
            normalize_sync_locator(
                "http://localhost:3000/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc"
            )
            .is_ok()
        );
        assert!(
            normalize_sync_locator(
                "http://127.0.0.1:3000/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc"
            )
            .is_ok()
        );
        assert!(
            normalize_sync_locator("http://[::1]:3000/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc")
                .is_ok()
        );
    }

    #[test]
    fn sync_handshake_rejects_an_incompatible_protocol_before_bootstrap() {
        let error = validate_handshake(&HandshakeResponse {
            protocol_version: crate::SERVER_PROTOCOL_VERSION + 1,
            sync_protocol_version: Some(crate::sync::SYNC_PROTOCOL_VERSION),
            lix_id: Some("01936f4e-7b6c-7c3d-8f9a-123456789abc".to_owned()),
            session_id: "session-1".to_owned(),
            active_account_id: "01920000-0000-7000-8000-000000000602".to_owned(),
        })
        .expect_err("incompatible protocol must fail");
        assert_eq!(error.code, "LIX_SERVER_PROTOCOL_ERROR");
        assert!(error.message.contains("unsupported"));
    }

    #[derive(Debug)]
    struct VersionMismatchClient;

    impl RawHttpClient for VersionMismatchClient {
        fn send(&self, _request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
            Box::pin(async {
                Ok(RawHttpResponse {
                    status: 200,
                    status_text: "OK".to_owned(),
                    body: serde_json::to_vec(&serde_json::json!({
                        "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                        "syncProtocolVersion": 999,
                        "lixId": "01936f4e-7b6c-7c3d-8f9a-123456789abc",
                        "sessionId": "session-from-incompatible-server",
                        "activeBranchId": "01920000-0000-7000-8000-000000001234",
                        "activeAccountId": crate::SYSTEM_ACCOUNT_ID,
                    }))
                    .expect("encode mismatched handshake"),
                })
            })
        }
    }

    #[derive(Debug)]
    struct MatchingClient;

    impl RawHttpClient for MatchingClient {
        fn send(&self, _request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
            Box::pin(async {
                Ok(RawHttpResponse {
                    status: 200,
                    status_text: "OK".to_owned(),
                    body: serde_json::to_vec(&serde_json::json!({
                        "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                        "syncProtocolVersion": crate::sync::SYNC_PROTOCOL_VERSION,
                        "lixId": "01936f4e-7b6c-7c3d-8f9a-123456789abc",
                        "sessionId": "session-from-server",
                        "activeBranchId": "01920000-0000-7000-8000-000000001234",
                        "activeAccountId": crate::SYSTEM_ACCOUNT_ID,
                    }))
                    .expect("encode handshake"),
                })
            })
        }
    }

    #[tokio::test]
    async fn sync_transport_retains_the_authority_lix_id() {
        let transport = HttpSyncTransport::connect_with(
            MatchingClient,
            "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
        )
        .await
        .expect("matching handshake should connect");
        assert_eq!(transport.lix_id(), "01936f4e-7b6c-7c3d-8f9a-123456789abc");
    }

    #[derive(Debug)]
    struct FenceHeaderClient {
        requests: Arc<Mutex<Vec<RawHttpRequest>>>,
    }

    impl RawHttpClient for FenceHeaderClient {
        fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
            Box::pin(async move {
                let handshake = {
                    let mut requests = self.requests.lock().expect("request log lock");
                    let handshake = requests.is_empty();
                    requests.push(request);
                    handshake
                };
                let body = if handshake {
                    serde_json::json!({
                        "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                        "syncProtocolVersion": crate::sync::SYNC_PROTOCOL_VERSION,
                        "lixId": "01936f4e-7b6c-7c3d-8f9a-123456789abc",
                        "sessionId": "session-from-server",
                        "activeAccountId": crate::SYSTEM_ACCOUNT_ID,
                    })
                } else {
                    serde_json::json!({ "kind": "delta", "cursor": 7, "events": [] })
                };
                Ok(RawHttpResponse {
                    status: 200,
                    status_text: "OK".to_owned(),
                    body: serde_json::to_vec(&body).expect("encode response"),
                })
            })
        }
    }

    #[tokio::test]
    async fn publication_fence_uses_private_non_waiting_preference() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let transport = HttpSyncTransport::connect_with(
            FenceHeaderClient {
                requests: Arc::clone(&requests),
            },
            "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
        )
        .await
        .expect("matching handshake should connect");
        transport
            .pull_now(Some(7), 1)
            .await
            .expect("publication fence should return immediately");
        let requests = requests.lock().expect("request log lock");
        let fence = requests.last().expect("fence request should be recorded");
        assert!(fence.url.ends_with("/sync/pull?after=7&limit=1"));
        assert!(fence.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("prefer") && value.eq_ignore_ascii_case("wait=0")
        }));
    }

    #[derive(Clone, Debug)]
    struct DescriptorClient {
        requests: Arc<Mutex<Vec<RawHttpRequest>>>,
        body: Vec<u8>,
    }

    impl RawHttpClient for DescriptorClient {
        fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
            Box::pin(async move {
                self.requests.lock().unwrap().push(request);
                Ok(RawHttpResponse {
                    status: 200,
                    status_text: "OK".into(),
                    body: self.body.clone(),
                })
            })
        }
    }

    fn descriptor_transport(
        descriptor: &crate::sync::PartialReplicaDescriptor,
    ) -> HttpSyncTransport<DescriptorClient> {
        HttpSyncTransport {
            client: DescriptorClient {
                requests: Arc::new(Mutex::new(Vec::new())),
                body: serde_json::to_vec(&crate::sync::LeasedPartialReplicaDescriptor::for_test(
                    descriptor.clone(),
                    crate::SYSTEM_ACCOUNT_ID,
                ))
                .unwrap(),
            },
            protocol_url: "https://sync.example/lix/v1/repository".into(),
            lix_id: descriptor.lix_id.clone(),
            session_id: "session".into(),
            active_account_id: crate::SYSTEM_ACCOUNT_ID.into(),
            baseline_lease: Arc::new(parking_lot::Mutex::new(Some(
                crate::sync::LeasedPartialReplicaDescriptor::for_test(
                    descriptor.clone(),
                    crate::SYSTEM_ACCOUNT_ID,
                )
                .lease,
            ))),
        }
    }

    #[test]
    fn candidate_deadline_charges_request_time_and_rejects_other_lease() {
        let id = uuid::Uuid::now_v7().to_string();
        let ttl = std::time::Duration::from_millis(crate::gc::NATIVE_BASELINE_LEASE_TTL_MS);
        let elapsed = super::CandidateBaselineDeadline::from_request_start(
            &id,
            web_time::Instant::now() - ttl,
        );
        assert_eq!(
            elapsed.check(&id).unwrap_err().code,
            "LIX_PARTIAL_CANDIDATE_EXPIRED"
        );
        let fresh =
            super::CandidateBaselineDeadline::from_request_start(&id, web_time::Instant::now());
        assert!(fresh.remaining().unwrap() <= ttl);
        fresh.check(&id).unwrap();
        assert!(fresh.check(&uuid::Uuid::now_v7().to_string()).is_err());
    }

    #[tokio::test]
    async fn candidate_deadline_does_not_trust_persisted_wall_clock_hint() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let mut transport = descriptor_transport(&descriptor);
        let mut wire: crate::sync::LeasedPartialReplicaDescriptor =
            serde_json::from_slice(&transport.client.body).unwrap();
        wire.lease.expires_at_ms = 1; // A large server/client clock offset.
        transport.client.body = serde_json::to_vec(&wire).unwrap();
        let received = transport.partial_replica_descriptor(None).await.unwrap();
        assert_eq!(received.wire.lease.expires_at_ms, 1);
        received
            .deadline
            .check(&received.wire.lease.lease_id)
            .unwrap();
        assert!(
            received.deadline.remaining().unwrap()
                <= std::time::Duration::from_millis(crate::gc::NATIVE_BASELINE_LEASE_TTL_MS)
        );
    }

    #[tokio::test]
    async fn baseline_renewal_client_bounds_reply_and_retains_binding_on_mismatch() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let mut transport = descriptor_transport(&descriptor);
        let prior = transport.baseline_lease.lock().clone().unwrap();
        transport.client.body = serde_json::to_vec(&prior).unwrap();
        assert_eq!(
            transport.renew_native_baseline_lease().await.unwrap(),
            prior
        );
        {
            let requests = transport.client.requests.lock().unwrap();
            assert_eq!(requests[0].response_limit, 1024);
            assert!(requests[0].url.ends_with("/sync/baseline-lease/renew"));
            let body: serde_json::Value =
                serde_json::from_slice(requests[0].body.as_ref().unwrap()).unwrap();
            assert_eq!(body["leaseId"], prior.lease_id);
        }
        let mut forged = serde_json::to_value(&prior).unwrap();
        forged["roots"] = serde_json::json!([uuid::Uuid::now_v7().to_string()]);
        transport.client.body = serde_json::to_vec(&forged).unwrap();
        assert_eq!(
            transport
                .renew_native_baseline_lease()
                .await
                .unwrap_err()
                .code,
            crate::sync::SYNC_PROTOCOL_MISMATCH_CODE
        );
        assert_eq!(transport.baseline_lease.lock().as_ref().unwrap(), &prior);
        transport.client.body = vec![b' '; 1025];
        assert!(transport.renew_native_baseline_lease().await.is_err());
        assert_eq!(transport.baseline_lease.lock().as_ref().unwrap(), &prior);
    }

    #[tokio::test]
    async fn baseline_transport_fork_preserves_original_binding_and_rejects_forged_reuse() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let transport = descriptor_transport(&descriptor);
        let old = transport.baseline_lease.lock().clone().unwrap();
        let candidate = crate::sync::LeasedPartialReplicaDescriptor::for_test(
            descriptor.clone(),
            crate::SYSTEM_ACCOUNT_ID,
        )
        .lease;
        let fork = transport.fork_native_baseline_lease(&candidate).unwrap();
        assert_eq!(
            transport.baseline_lease.lock().as_ref().unwrap().lease_id,
            old.lease_id
        );
        assert_eq!(
            fork.baseline_lease.lock().as_ref().unwrap().lease_id,
            candidate.lease_id
        );
        let mut forged = serde_json::to_value(&old).unwrap();
        forged["roots"] = serde_json::json!([uuid::Uuid::now_v7().to_string()]);
        let forged: crate::gc::NativeBaselineLease = serde_json::from_value(forged).unwrap();
        assert!(transport.bind_native_baseline_lease(&forged).is_err());
        assert_eq!(transport.baseline_lease.lock().as_ref().unwrap(), &old);
    }

    #[tokio::test]
    async fn partial_replica_descriptor_fetch_uses_versioned_session_request() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let transport = descriptor_transport(&descriptor);
        let branch_id = descriptor.selected_branch.branch_id.as_str();
        assert_eq!(
            transport
                .partial_replica_descriptor(Some(branch_id))
                .await
                .unwrap()
                .wire
                .descriptor,
            descriptor
        );
        let requests = transport.client.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0]
                .url
                .ends_with(&format!("/sync/descriptor?branchId={branch_id}"))
        );
        assert_eq!(requests[0].method, http::Method::GET);
        assert_eq!(
            requests[0].response_limit,
            crate::sync::MAX_LEASED_DESCRIPTOR_BYTES
        );
        assert!(
            requests[0]
                .headers
                .iter()
                .any(|(key, value)| key == super::SESSION_HEADER && value == "session")
        );
        assert!(requests[0].headers.iter().any(|(key, value)| key
            == crate::sync::SYNC_PROTOCOL_VERSION_HEADER
            && value == &crate::sync::SYNC_PROTOCOL_VERSION.to_string()));
    }

    #[tokio::test]
    async fn descriptor_wait_encodes_cursor_and_rejects_regression() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let transport = descriptor_transport(&descriptor);
        let branch = &descriptor.selected_branch.branch_id;
        assert_eq!(
            transport
                .wait_partial_replica_descriptor(branch, descriptor.cursor)
                .await
                .unwrap()
                .wire
                .descriptor,
            descriptor
        );
        {
            let requests = transport.client.requests.lock().unwrap();
            assert!(requests[0].url.ends_with(&format!(
                "/sync/descriptor?branchId={branch}&after={}",
                descriptor.cursor
            )));
            assert_eq!(
                requests[0].response_limit,
                crate::sync::MAX_LEASED_DESCRIPTOR_BYTES
            );
        }
        assert!(
            transport
                .wait_partial_replica_descriptor(branch, descriptor.cursor + 1)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn partial_replica_descriptor_fetch_rejects_mismatched_metadata() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let mut wrong_version = descriptor.clone();
        wrong_version.descriptor_version += 1;
        let mut wrong_branch = descriptor.clone();
        wrong_branch.selected_branch.branch_id = crate::GLOBAL_BRANCH_ID.to_owned();
        let mut wrong_global = descriptor.clone();
        wrong_global.global_branch.branch_id = descriptor.default_branch_id.clone();
        let mut wrong_default = descriptor.clone();
        wrong_default.default_branch_id = "invalid".into();
        wrong_default.selected_branch.branch_id = "invalid".into();
        let mut wrong_commit = descriptor.clone();
        wrong_commit.selected_branch.checkpoint.commit_id = "invalid".into();
        let mut missing_digest = descriptor.clone();
        missing_digest.selected_branch.head.scoped_range_root_id = Some([1; 32]);
        missing_digest.selected_branch.head.scoped_range_root_digest = None;
        let mut zero_root = descriptor.clone();
        zero_root.global_branch.head.row_pk_index_root_id = Some([0; 32]);
        let mut wrong_created_at = descriptor.clone();
        wrong_created_at.selected_branch.created_at = "not-a-timestamp".into();
        let mut wrong_updated_at = descriptor.clone();
        wrong_updated_at.global_branch.updated_at = "2026-09-10".into();
        let mut wrong_change_id = descriptor.clone();
        wrong_change_id.selected_branch.ref_change_id = "not-a-change-id".into();
        for invalid in [
            wrong_created_at,
            wrong_updated_at,
            wrong_change_id,
            wrong_version,
            wrong_branch,
            wrong_global,
            wrong_default,
            wrong_commit,
            missing_digest,
            zero_root,
        ] {
            assert_eq!(
                descriptor_transport(&invalid)
                    .partial_replica_descriptor(None)
                    .await
                    .unwrap_err()
                    .code,
                crate::sync::SYNC_PROTOCOL_MISMATCH_CODE
            );
        }
        let mut transport = descriptor_transport(&descriptor);
        transport.lix_id = "different-repository".into();
        assert_eq!(
            transport
                .partial_replica_descriptor(None)
                .await
                .unwrap_err()
                .code,
            crate::sync::SYNC_REPOSITORY_ID_MISMATCH_CODE
        );
        let mut transport = descriptor_transport(&descriptor);
        transport.client.body = vec![b' '; crate::sync::MAX_LEASED_DESCRIPTOR_BYTES + 1];
        assert_eq!(
            transport
                .partial_replica_descriptor(None)
                .await
                .unwrap_err()
                .code,
            crate::sync::SYNC_PROTOCOL_MISMATCH_CODE
        );
        let transport = descriptor_transport(&descriptor);
        assert!(
            transport
                .partial_replica_descriptor(Some("invalid"))
                .await
                .is_err()
        );
        assert!(transport.client.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn native_object_fetch_validates_ordered_bytes_and_sets_receive_limit() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let bytes = b"native transport fixture".to_vec();
        let address = crate::tracked_state::NativeObjectRef::TrackedStateTreeChunk(
            *blake3::hash(&bytes).as_bytes(),
        );
        let response = crate::sync::native_object::NativeObjectResponse {
            lix_id: descriptor.lix_id.clone(),
            objects: vec![crate::sync::native_object::NativeObject {
                address,
                bytes: bytes.clone(),
            }],
        };
        let mut transport = descriptor_transport(&descriptor);
        transport.client.body = serde_json::to_vec(&response).unwrap();
        let fetched = transport.native_objects(&[address]).await.unwrap();
        assert_eq!(fetched.objects[0].bytes, bytes);
        {
            let requests = transport.client.requests.lock().unwrap();
            assert!(requests[0].url.ends_with("/sync/native-objects"));
            assert_eq!(requests[0].method, http::Method::POST);
            assert_eq!(
                requests[0].response_limit,
                crate::sync::native_object::MAX_NATIVE_OBJECT_RESPONSE_BYTES
            );
        }
        let mut corrupt = response;
        corrupt.objects[0].bytes.push(0);
        transport.client.body = serde_json::to_vec(&corrupt).unwrap();
        assert!(transport.native_objects(&[address]).await.is_err());
    }

    #[tokio::test]
    async fn native_object_range_fetch_checks_bounds_and_identity() {
        use crate::sync::native_object_range::{
            NativeObjectRangeRequest, NativeObjectRangeResponse,
        };
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let address = crate::tracked_state::NativeObjectRef::TrackedStateTreeChunk([1; 32]);
        let range = NativeObjectRangeRequest {
            address,
            offset: 2,
            max_bytes: 3,
        };
        let response = NativeObjectRangeResponse {
            lix_id: descriptor.lix_id.clone(),
            address,
            offset: 2,
            total_bytes: 8,
            bytes: b"cde".to_vec(),
        };
        let mut transport = descriptor_transport(&descriptor);
        transport.client.body = serde_json::to_vec(&response).unwrap();
        assert_eq!(
            transport.native_object_range(&range).await.unwrap().bytes,
            b"cde"
        );
        {
            let requests = transport.client.requests.lock().unwrap();
            assert!(requests[0].url.ends_with("/sync/native-object-range"));
            assert_eq!(requests[0].method, http::Method::POST);
            assert_eq!(
                requests[0].response_limit,
                crate::sync::native_object::MAX_NATIVE_OBJECT_RESPONSE_BYTES
            );
        }
        for mutation in 0..6 {
            let mut wrong = response.clone();
            match mutation {
                0 => wrong.offset += 1,
                1 => wrong.total_bytes = 1,
                2 => {
                    wrong.bytes.pop();
                }
                3 => wrong.bytes.push(0),
                4 => wrong.lix_id = "another-repository".into(),
                _ => {
                    wrong.address = crate::tracked_state::NativeObjectRef::ScopedRangeNode([1; 32])
                }
            }
            transport.client.body = serde_json::to_vec(&wrong).unwrap();
            assert!(transport.native_object_range(&range).await.is_err());
        }
        let transport = descriptor_transport(&descriptor);
        for (offset, max_bytes) in [(0, 0), (0, 1048577), (u64::MAX, 1)] {
            assert!(
                transport
                    .native_object_range(&NativeObjectRangeRequest {
                        address,
                        offset,
                        max_bytes
                    })
                    .await
                    .is_err()
            );
        }
        assert!(transport.client.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn native_object_fetch_rejects_invalid_requests_before_network() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let transport = descriptor_transport(&descriptor);
        assert!(transport.native_objects(&[]).await.is_err());
        let address = crate::tracked_state::NativeObjectRef::TrackedStateTreeChunk([1; 32]);
        assert!(transport.native_objects(&[address, address]).await.is_err());
        assert!(transport.native_objects(&vec![address; 33]).await.is_err());
        assert!(transport.client.requests.lock().unwrap().is_empty());
    }

    #[derive(Debug)]
    struct MissingVersionClient;

    impl RawHttpClient for MissingVersionClient {
        fn send(&self, _request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
            Box::pin(async {
                Ok(RawHttpResponse {
                    status: 200,
                    status_text: "OK".to_owned(),
                    body: serde_json::to_vec(&serde_json::json!({
                        "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                        "lixId": "01936f4e-7b6c-7c3d-8f9a-123456789abc",
                        "sessionId": "session-from-legacy-server",
                        "activeBranchId": "01920000-0000-7000-8000-000000001234",
                        "activeAccountId": crate::SYSTEM_ACCOUNT_ID,
                    }))
                    .expect("encode legacy handshake"),
                })
            })
        }
    }

    #[derive(Debug)]
    struct MissingIdentityClient;

    impl RawHttpClient for MissingIdentityClient {
        fn send(&self, _request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
            Box::pin(async {
                Ok(RawHttpResponse {
                    status: 200,
                    status_text: "OK".to_owned(),
                    body: serde_json::to_vec(&serde_json::json!({
                        "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                        "syncProtocolVersion": crate::sync::SYNC_PROTOCOL_VERSION,
                        "sessionId": "session-from-legacy-server",
                        "activeBranchId": "01920000-0000-7000-8000-000000001234",
                        "activeAccountId": crate::SYSTEM_ACCOUNT_ID,
                    }))
                    .expect("encode legacy handshake"),
                })
            })
        }
    }

    #[tokio::test]
    async fn sync_handshake_rejects_a_mismatched_sync_protocol_version() {
        let error = HttpSyncTransport::connect_with(
            VersionMismatchClient,
            "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
        )
        .await
        .expect_err("an incompatible sync protocol must fail before transfer");
        assert_eq!(error.code, crate::sync::SYNC_PROTOCOL_MISMATCH_CODE);
        assert_eq!(
            error.details,
            Some(serde_json::json!({
                "clientSyncProtocolVersion": crate::sync::SYNC_PROTOCOL_VERSION,
                "serverSyncProtocolVersion": 999,
            }))
        );
    }

    #[tokio::test]
    async fn sync_handshake_rejects_a_missing_sync_protocol_version() {
        let error = HttpSyncTransport::connect_with(
            MissingVersionClient,
            "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
        )
        .await
        .expect_err("a legacy server must fail before transfer");
        assert_eq!(error.code, crate::sync::SYNC_PROTOCOL_MISMATCH_CODE);
        assert_eq!(
            error.details,
            Some(serde_json::json!({
                "clientSyncProtocolVersion": crate::sync::SYNC_PROTOCOL_VERSION,
                "serverSyncProtocolVersion": null,
            }))
        );
    }

    #[tokio::test]
    async fn sync_handshake_rejects_a_missing_repository_identity_as_terminal() {
        let error = HttpSyncTransport::connect_with(
            MissingIdentityClient,
            "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
        )
        .await
        .expect_err("a legacy server must fail before transfer");
        assert_eq!(error.code, crate::sync::SYNC_PROTOCOL_MISMATCH_CODE);
        assert_eq!(
            error.details,
            Some(serde_json::json!({ "missingField": "lixId" }))
        );
    }

    #[test]
    fn session_and_protocol_headers_are_reserved_for_the_transport() {
        assert!(HttpSyncTransport::<VersionMismatchClient>::is_reserved_header("Lix-Session-Id"));
        assert!(
            HttpSyncTransport::<VersionMismatchClient>::is_reserved_header(
                "LIX-SYNC-PROTOCOL-VERSION"
            )
        );
        assert!(!HttpSyncTransport::<VersionMismatchClient>::is_reserved_header("Authorization"));
    }

    #[test]
    fn query_encoding_is_rfc3986_component_encoding() {
        assert_eq!(encode_query("a b/c?d=ü"), "a%20b%2Fc%3Fd%3D%C3%BC");
    }

    #[test]
    fn structured_http_error_preserves_remote_details_and_adds_status() {
        let error = response_error(
            &RawHttpResponse {
                status: 503,
                status_text: "Service Unavailable".to_owned(),
                body: serde_json::to_vec(&serde_json::json!({
                    "error": {
                        "code": "LIX_ERROR_LIX_MIGRATING",
                        "message": "The lix repository is being migrated.",
                        "hint": "Retry after the migration completes.",
                        "details": {
                            "fromVersion": 68,
                            "toVersion": 71,
                            "retryable": true,
                        },
                    },
                }))
                .expect("encode error response"),
            },
            "open sync session",
        );

        assert_eq!(error.code, "LIX_ERROR_LIX_MIGRATING");
        assert_eq!(
            error.hint.as_deref(),
            Some("Retry after the migration completes.")
        );
        assert_eq!(
            error.details,
            Some(serde_json::json!({
                "fromVersion": 68,
                "toVersion": 71,
                "retryable": true,
                "httpStatus": 503,
            }))
        );
    } // Insert within sync/http.rs tests (uses existing DescriptorClient).
    #[tokio::test]
    async fn restart_transport_authenticates_exact_outcome_and_enforces_receive_cap() {
        let lix = crate::open_lix().await.unwrap();
        let descriptor = lix.partial_replica_descriptor(None).await.unwrap();
        let request = super::super::PartialAttemptRestartRequest {
            old: super::super::PartialMergeRequest {
                attempt_id: uuid::Uuid::now_v7().to_string(),
                branch_id: descriptor.selected_branch.branch_id.clone(),
                base_commit_id: descriptor.selected_branch.head.commit_id.clone(),
                expected_authority_head_commit_id: descriptor
                    .selected_branch
                    .head
                    .commit_id
                    .clone(),
                captured_local_head_commit_id: uuid::Uuid::now_v7().to_string(),
                checkpoint_commit_id: descriptor.selected_branch.checkpoint.commit_id.clone(),
                global_head_commit_id: descriptor.global_branch.head.commit_id.clone(),
                global_checkpoint_commit_id: descriptor.global_branch.checkpoint.commit_id.clone(),
            },
            next_attempt_id: uuid::Uuid::now_v7().to_string(),
        };
        let mut transport = descriptor_transport(&descriptor);
        let outcome = super::super::PartialAttemptRestartOutcome::Committed {
            repository_id: transport.lix_id.clone(),
            account_id: transport.active_account_id.clone(),
            receipt: super::super::PartialMergeReceipt {
                request: request.old.clone(),
                merge_commit_id: uuid::Uuid::now_v7().to_string(),
            },
        };
        transport.client.body = serde_json::to_vec(&outcome).unwrap();
        assert_eq!(
            transport.restart_partial_attempt(&request).await.unwrap(),
            outcome
        );
        {
            let sent = transport.client.requests.lock().unwrap();
            let sent = sent.last().unwrap();
            assert!(sent.url.ends_with("/sync/merge/restart"));
            assert_eq!(sent.response_limit, 4096);
        }
        let mut wrong = serde_json::to_value(&outcome).unwrap();
        wrong["accountId"] = uuid::Uuid::now_v7().to_string().into();
        transport.client.body = serde_json::to_vec(&wrong).unwrap();
        assert!(transport.restart_partial_attempt(&request).await.is_err());
        transport.client.body = vec![b' '; 4097];
        assert!(transport.restart_partial_attempt(&request).await.is_err());
    }

    #[tokio::test]
    async fn partial_merge_transport_preserves_exact_receipt_and_receive_bounds() {
        let lix = crate::open_lix().await.unwrap();
        let descriptor = lix.partial_replica_descriptor(None).await.unwrap();
        let request = super::super::PartialMergeRequest {
            attempt_id: uuid::Uuid::now_v7().to_string(),
            branch_id: descriptor.selected_branch.branch_id.clone(),
            base_commit_id: descriptor.selected_branch.head.commit_id.clone(),
            expected_authority_head_commit_id: descriptor.selected_branch.head.commit_id.clone(),
            captured_local_head_commit_id: uuid::Uuid::now_v7().to_string(),
            checkpoint_commit_id: descriptor.selected_branch.checkpoint.commit_id.clone(),
            global_head_commit_id: descriptor.global_branch.head.commit_id.clone(),
            global_checkpoint_commit_id: descriptor.global_branch.checkpoint.commit_id.clone(),
        };
        let receipt = super::super::PartialMergeReceipt {
            request: request.clone(),
            merge_commit_id: uuid::Uuid::now_v7().to_string(),
        };
        let mut transport = descriptor_transport(&descriptor);
        transport.client.body = serde_json::to_vec(&receipt).unwrap();
        assert_eq!(
            transport.merge_partial_replica(&request).await.unwrap(),
            receipt
        );
        {
            let requests = transport.client.requests.lock().unwrap();
            let sent = requests.last().unwrap();
            assert!(sent.url.ends_with("/sync/merge"));
            assert_eq!(sent.response_limit, 4096);
            assert!(
                sent.headers
                    .iter()
                    .any(|(key, value)| key == "lix-sync-protocol-version"
                        && value == &super::super::SYNC_PROTOCOL_VERSION.to_string())
            );
        }
        let mut changed = receipt;
        changed.request.captured_local_head_commit_id = uuid::Uuid::now_v7().to_string();
        transport.client.body = serde_json::to_vec(&changed).unwrap();
        assert!(transport.merge_partial_replica(&request).await.is_err());
    }
}
