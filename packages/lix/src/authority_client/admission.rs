//! Read-only hosted admission uses the same Rust migration wait policy as open.
use super::*;
use futures_util::{
    StreamExt,
    future::{Either, select},
};
use std::time::Duration;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolAdmissionIdentity {
    pub repository_id: String,
    pub principal_id: String,
    pub storage_epoch: u32,
    pub protocol_epoch: u32,
}

/// Authenticate repository identity without creating a remote SQL session.
/// Bodies are limited to 16 KiB. Transient admission failures get at most five
/// attempts, each bounded to five seconds, with 500ms–4s exponential backoff.
/// Typed migration waits retain their existing caller-cancellable policy.
pub async fn admit_protocol_client<H: ProtocolHttp + Clone + 'static>(
    http: H,
    base_url: impl Into<String>,
    progress: Option<Arc<dyn crate::OpenProgressSink>>,
) -> Result<(ProtocolAdmissionIdentity, crate::OpenReport), LixError> {
    let base_url = base_url.into();
    let normalized = normalize_protocol_base_url(base_url.trim_end_matches('/'))
        .map_err(|error| LixError::new("LIX_TRANSPORT_CONTRACT", error.message))?;
    let repository_id = url::Url::parse(&normalized)
        .map_err(|_| admission_protocol_error())?
        .path_segments()
        .and_then(|mut parts| parts.nth(2))
        .ok_or_else(admission_protocol_error)?
        .to_owned();
    let migrations = Arc::new(std::sync::Mutex::new(Vec::<crate::OpenMigration>::new()));
    let captured = Arc::clone(&migrations);
    let observer: Arc<dyn crate::OpenProgressSink> = Arc::new(
        crate::CallbackOpenProgressSink::new(move |event: crate::OpenProgress| {
            if let Some(from_format) = event.from_format {
                let mut observed = captured.lock().unwrap_or_else(|error| error.into_inner());
                if let Some(first) = observed.first_mut() {
                    first.from_format = first.from_format.min(from_format);
                    first.to_format = first.to_format.max(event.to_format);
                } else {
                    observed.push(crate::OpenMigration {
                        scope: crate::OpenScope::Authority,
                        from_format,
                        to_format: event.to_format,
                    });
                }
            }
            crate::open_types::emit_open_progress(progress.as_ref(), event);
        }),
    );
    let snapshot = |phase| crate::OpenProgress {
        scope: crate::OpenScope::Authority,
        phase,
        from_format: None,
        to_format: crate::CURRENT_STORAGE_FORMAT_VERSION,
        completed: None,
        total: None,
    };
    crate::open_types::emit_open_progress(Some(&observer), snapshot(crate::OpenPhase::Inspecting));
    let mut retries = 0_u32;
    let identity = loop {
        let response = {
            let request = admission_response(&http, &normalized);
            let timeout = http.sleep(Duration::from_secs(5));
            futures_util::pin_mut!(request, timeout);
            match select(request, timeout).await {
                Either::Left((result, _)) => result,
                Either::Right(_) => Err(LixError::new(
                    "LIX_ADMISSION_TIMEOUT",
                    "Repository admission timed out",
                )),
            }
        };
        let response = match response {
            Ok(response) => response,
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "LIX_TRANSPORT_NETWORK" | "LIX_REMOTE_UNAVAILABLE" | "LIX_ADMISSION_TIMEOUT"
                ) =>
            {
                admission_backoff(&http, &mut retries, Some(error)).await?;
                continue;
            }
            Err(error) => return Err(error),
        };
        if !is_success_status(response.status) {
            let error = error_from_http_response(&response);
            if let Some(delay) = opening_migration_retry_delay(&error) {
                report_authority_migration(&error, Some(&observer));
                http.sleep(delay).await;
                continue;
            }
            if matches!(response.status, 502 | 503 | 504) {
                admission_backoff(&http, &mut retries, None).await?;
                continue;
            }
            return Err(match response.status {
                401 | 403 => LixError::new(
                    "LIX_ADMISSION_AUTH_REJECTED",
                    "Authority rejected repository admission",
                ),
                409 | 426 => LixError::new(
                    "LIX_ADMISSION_EPOCH",
                    "Repository is incompatible with this client version",
                ).with_details(serde_json::json!({
                    "httpStatus": response.status,
                    "expectedStorageEpoch": crate::CURRENT_STORAGE_FORMAT_VERSION,
                    "expectedProtocolEpoch": crate::SYNC_PROTOCOL_VERSION,
                })),
                status => LixError::new(
                    "LIX_ADMISSION_HTTP",
                    format!("Authority admission returned HTTP {status}"),
                ),
            });
        }
        let identity: ProtocolAdmissionIdentity =
            serde_json::from_slice(&response.body).map_err(|_| admission_protocol_error())?;
        if identity.repository_id != repository_id
            || !(1..=255).contains(&identity.principal_id.len())
            || !identity
                .principal_id
                .bytes()
                .all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(admission_protocol_error());
        }
        if identity.storage_epoch != crate::CURRENT_STORAGE_FORMAT_VERSION
            || identity.protocol_epoch != crate::SYNC_PROTOCOL_VERSION
        {
            return Err(LixError::new(
                "LIX_ADMISSION_EPOCH",
                "Repository is incompatible with this client version",
            ).with_details(serde_json::json!({
                "storageEpoch": identity.storage_epoch,
                "protocolEpoch": identity.protocol_epoch,
                "expectedStorageEpoch": crate::CURRENT_STORAGE_FORMAT_VERSION,
                "expectedProtocolEpoch": crate::SYNC_PROTOCOL_VERSION,
            })));
        }
        break identity;
    };
    crate::open_types::emit_open_progress(Some(&observer), snapshot(crate::OpenPhase::Complete));
    let migrations = migrations
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    let report = crate::OpenReport {
        format: identity.storage_epoch,
        initialized: false,
        migration: None,
        migrations,
    };
    Ok((identity, report))
}

fn admission_protocol_error() -> LixError {
    LixError::new(
        "LIX_ADMISSION_PROTOCOL",
        "Authority returned invalid or mismatched admission metadata",
    )
}

/// Keep network errors recognizable for verified offline admission, but mark
/// the exhausted budget so callers do not automatically restart it.
async fn admission_backoff<H: ProtocolHttp>(
    http: &H,
    retries: &mut u32,
    error: Option<LixError>,
) -> Result<(), LixError> {
    if *retries == 4 {
        return Err(error
            .unwrap_or_else(|| {
                LixError::new(
                    "LIX_ADMISSION_UNAVAILABLE",
                    "Repository service is temporarily unavailable. Please retry.",
                )
            })
            .with_details(serde_json::json!({"admissionRetryExhausted": true})));
    }
    http.sleep(Duration::from_millis(500 << *retries)).await;
    *retries += 1;
    Ok(())
}

struct AdmissionCancel(Option<http::StreamCancel>);
impl Drop for AdmissionCancel {
    fn drop(&mut self) {
        if let Some(cancel) = self.0.take() {
            cancel();
        }
    }
}

async fn admission_response<H: ProtocolHttp>(
    http: &H,
    normalized: &str,
) -> Result<ProtocolHttpResponse, LixError> {
    let mut response = http
        .request_stream(ProtocolHttpRequest {
            method: "GET".into(),
            url: format!("{normalized}admission"),
            headers: vec![(
                "lix-sync-protocol-version".into(),
                crate::SYNC_PROTOCOL_VERSION.to_string(),
            )],
            body: None,
        })
        .await?;
    let mut cancel = AdmissionCancel(Some(response.cancel));
    let mut body = Vec::new();
    while let Some(chunk) = response.body.next().await {
        let chunk = chunk?;
        if body.len().saturating_add(chunk.len()) > 16 * 1024 {
            // Gateway HTML is not admission metadata. Cancel the oversized
            // body while retaining the status for bounded transient retries.
            if matches!(response.status, 502 | 503 | 504) {
                return Ok(ProtocolHttpResponse {
                    status: response.status,
                    headers: response.headers,
                    body: Bytes::new(),
                });
            }
            return Err(admission_protocol_error());
        }
        body.extend_from_slice(&chunk);
    }
    cancel.0 = None;
    Ok(ProtocolHttpResponse {
        status: response.status,
        headers: response.headers,
        body: Bytes::from(body),
    })
}
