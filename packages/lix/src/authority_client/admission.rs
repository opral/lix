//! Read-only hosted admission uses the same Rust migration wait policy as open.
use super::*;
use futures_util::StreamExt;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolAdmissionIdentity {
    pub repository_id: String,
    pub principal_id: String,
    pub storage_epoch: u32,
    pub protocol_epoch: u32,
}

/// Authenticate repository identity without creating a remote SQL session.
/// Bodies are limited to 16 KiB and only typed migration responses are retried.
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
    let identity = loop {
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
        let mut body = Vec::new();
        while let Some(chunk) = response.body.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    (response.cancel)();
                    return Err(error);
                }
            };
            if body.len().saturating_add(chunk.len()) > 16 * 1024 {
                (response.cancel)();
                return Err(admission_protocol_error());
            }
            body.extend_from_slice(&chunk);
        }
        let response = ProtocolHttpResponse {
            status: response.status,
            headers: response.headers,
            body: Bytes::from(body),
        };
        if !is_success_status(response.status) {
            let error = error_from_http_response(&response);
            if let Some(delay) = opening_migration_retry_delay(&error) {
                report_authority_migration(&error, Some(&observer));
                http.sleep(delay).await;
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
                ),
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
            ));
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
