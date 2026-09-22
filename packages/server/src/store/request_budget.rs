//! Keep interactive reads bounded without applying their short deadline to
//! whole immutable-segment uploads. Both clients address the same store.
use std::{fmt, sync::Arc};

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};

#[derive(Debug)]
pub(super) struct UploadBudgetStore {
    pub reads: Arc<dyn ObjectStore>,
    pub uploads: Arc<dyn ObjectStore>,
}

impl fmt::Display for UploadBudgetStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "upload-budget {}", self.reads)
    }
}

#[async_trait]
impl ObjectStore for UploadBudgetStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.uploads.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.uploads.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        self.reads.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.reads.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.reads.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.reads.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.uploads.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{S3RequestBudget, s3_client_options, s3_retry_config};
    use axum::{
        Router,
        body::Body,
        http::{Request, Response},
    };
    use object_store::{ObjectStoreExt, PutMode, aws::AmazonS3Builder};
    use std::time::Duration;

    #[tokio::test]
    async fn slow_upload_keeps_create_precondition_while_reads_still_time_out() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (sent, received) = tokio::sync::oneshot::channel();
        let sent = Arc::new(tokio::sync::Mutex::new(Some(sent)));
        let app = Router::new().fallback(move |request: Request<Body>| {
            let sent = Arc::clone(&sent);
            async move {
                if request.method() == "PUT" {
                    assert_eq!(request.headers().get("if-none-match").unwrap(), "*");
                    let bytes = axum::body::to_bytes(request.into_body(), 1024)
                        .await
                        .unwrap();
                    assert_eq!(bytes.as_ref(), b"migration segment");
                    if let Some(sent) = sent.lock().await.take() {
                        let _ = sent.send(());
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
                Response::builder()
                    .header("etag", "\"fixture\"")
                    .body(Body::empty())
                    .unwrap()
            }
        });
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = |timeout| -> Arc<dyn ObjectStore> {
            let budget = S3RequestBudget {
                request_timeout: timeout,
                connect_timeout: Duration::from_secs(1),
                retry_timeout: timeout,
                max_retries: 0,
            };
            Arc::new(
                AmazonS3Builder::new()
                    .with_endpoint(&endpoint)
                    .with_bucket_name("fixture")
                    .with_access_key_id("test")
                    .with_secret_access_key("test")
                    .with_region("auto")
                    .with_client_options(s3_client_options(budget))
                    .with_allow_http(true)
                    .with_retry(s3_retry_config(budget))
                    .build()
                    .unwrap(),
            )
        };
        let store = UploadBudgetStore {
            reads: client(Duration::from_millis(50)),
            uploads: client(Duration::from_secs(2)),
        };
        let path = Path::from("segment");
        store
            .put_opts(
                &path,
                PutPayload::from_static(b"migration segment"),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .expect("upload must outlive the interactive read budget");
        received.await.unwrap();
        assert!(
            store.head(&path).await.is_err(),
            "HEAD must retain the short read deadline"
        );
        server.abort();
        let _ = server.await;
    }
}
