use bytes::Bytes;
use futures_util::StreamExt;

use crate::LixError;

use super::http::StreamCancel;
use super::{
    ClientCore, ProtocolByteStream, ProtocolHttp, ProtocolHttpRequest, ProtocolHttpResponse,
    error_from_http_response, is_success_status,
};

/// A streaming repository snapshot. Dropping or cancelling it releases the HTTP request.
pub struct ProtocolSnapshotExport {
    body: Option<ProtocolByteStream>,
    cancel: Option<StreamCancel>,
}

impl std::fmt::Debug for ProtocolSnapshotExport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtocolSnapshotExport")
            .field("active", &self.body.is_some())
            .finish_non_exhaustive()
    }
}

impl ProtocolSnapshotExport {
    pub async fn next(&mut self) -> Result<Option<Bytes>, LixError> {
        let Some(body) = self.body.as_mut() else {
            return Ok(None);
        };
        match body.next().await {
            Some(Ok(chunk)) => Ok(Some(chunk)),
            Some(Err(error)) => {
                self.cancel();
                Err(error)
            }
            None => {
                self.cancel();
                Ok(None)
            }
        }
    }

    pub fn cancel(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel();
        }
        self.body = None;
    }
}

impl Drop for ProtocolSnapshotExport {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl<H: ProtocolHttp> ClientCore<H> {
    /// Exports the whole repository, independent of the session's active branch.
    pub async fn export_snapshot(&self) -> Result<ProtocolSnapshotExport, LixError> {
        self.ensure_usable()?;
        let response = self
            .http()
            .request_stream(ProtocolHttpRequest {
                method: "GET".to_owned(),
                url: self.join_path("snapshot")?,
                headers: vec![(
                    "accept".to_owned(),
                    "application/vnd.lix.snapshot".to_owned(),
                )],
                body: None,
            })
            .await?;
        let status = response.status;
        let headers = response.headers;
        let mut export = ProtocolSnapshotExport {
            body: Some(response.body),
            cancel: Some(response.cancel),
        };
        if !is_success_status(status) {
            // Only error envelopes are buffered; successful snapshots remain streamed.
            // Bound the retained body even if a proxy sends an unexpectedly large error.
            let mut body = Vec::new();
            const MAX_ERROR_BYTES: usize = 64 * 1024;
            while let Some(chunk) = export.next().await? {
                let remaining = MAX_ERROR_BYTES - body.len();
                body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                if body.len() == MAX_ERROR_BYTES {
                    break;
                }
            }
            return Err(error_from_http_response(&ProtocolHttpResponse {
                status,
                headers,
                body: Bytes::from(body),
            }));
        }
        Ok(export)
    }
}
