//! An `ObjectStore` wrapper that makes a local directory behave like a remote
//! one, so a benchmark can measure the profile SlateDB actually ships against.
//!
//! SlateDB's target is object storage. Every benchmark in this repository runs
//! it over `LocalFileSystem`, where a PUT is a `write(2)` into page cache: the
//! per-byte cost of durability is close to zero and CPU is therefore ~100% of
//! the measured write. That is the wrong denominator for any change that trades
//! CPU for bytes, because it prices the bytes at nothing.
//!
//! This wrapper restores the two terms a real object store charges:
//!
//! * **Per request** — one round trip, applied to every operation.
//! * **Per byte** — transfer time at a fixed link rate, applied to bytes that
//!   actually cross the boundary in either direction.
//!
//! Both are applied with `tokio::time::sleep`, so the delay yields the executor
//! rather than burning a core: concurrent requests overlap exactly as they would
//! against a real endpoint, and background compaction keeps making progress
//! while a foreground write waits.
//!
//! Read bytes are billed from `GetResult::range` rather than by draining the
//! stream, so the wrapper never buffers an object and never changes the memory
//! profile of the code under test.
//!
//! ## Accuracy
//!
//! Tokio's timer wheel has ~1 ms granularity, so an individual sleep rounds up
//! to the next millisecond. Choose a round-trip that is a comfortable multiple
//! of 1 ms (the profiles below are 15 ms and 80 ms) and the per-request error
//! stays under ~7%. It is also common-mode: both arms of an A/B pay it
//! identically, so it cannot manufacture a delta.
//!
//! This models latency and bandwidth. It does not model request throttling,
//! tail latency, retries, or multipart concurrency limits — a real endpoint is
//! worse than this wrapper, not better, so the numbers it produces are a
//! conservative bound on the remote win.

use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{BoxStream, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult, UploadPart,
};

/// A link description: one round trip per request, plus transfer at a rate.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RemoteProfile {
    pub round_trip: Duration,
    pub megabits_per_second: f64,
}

impl RemoteProfile {
    /// Same-region object storage from a compute instance.
    pub const REGIONAL: Self = Self {
        round_trip: Duration::from_millis(15),
        megabits_per_second: 600.0,
    };

    /// Object storage reached over a wide-area link — cross-region, or a
    /// developer machine talking to a bucket.
    pub const WIDE_AREA: Self = Self {
        round_trip: Duration::from_millis(80),
        megabits_per_second: 100.0,
    };

    /// Parses `<rtt_ms>:<mbps>`, or one of the named profiles.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "local" | "" => return None,
            "regional" => return Some(Self::REGIONAL),
            "wide-area" => return Some(Self::WIDE_AREA),
            _ => {}
        }
        let (rtt, mbps) = value.split_once(':')?;
        Some(Self {
            round_trip: Duration::from_secs_f64(rtt.parse::<f64>().ok()? / 1000.0),
            megabits_per_second: mbps.parse().ok()?,
        })
    }

    /// Reads `LIX_CAS_REMOTE_PROFILE`. Absent or `local` means no wrapper, so
    /// the benchmark keeps using the production local open path unchanged.
    pub fn from_env() -> Option<Self> {
        Self::parse(&std::env::var("LIX_CAS_REMOTE_PROFILE").ok()?)
    }

    pub fn label(&self) -> String {
        format!(
            "{}ms@{}mbps",
            self.round_trip.as_secs_f64() * 1000.0,
            self.megabits_per_second
        )
    }

    fn transfer(&self, bytes: u64) -> Duration {
        Duration::from_secs_f64((bytes as f64 * 8.0) / (self.megabits_per_second * 1_000_000.0))
    }

    /// Round trip plus transfer, the cost of moving `bytes` in one request.
    pub fn request(&self, bytes: u64) -> Duration {
        self.round_trip + self.transfer(bytes)
    }
}

/// Wraps any store so its operations cost what they would over `profile`.
#[derive(Debug)]
pub struct RemoteObjectStore {
    inner: Arc<dyn ObjectStore>,
    profile: RemoteProfile,
}

impl RemoteObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, profile: RemoteProfile) -> Self {
        Self { inner, profile }
    }

    async fn pay(&self, bytes: u64) {
        tokio::time::sleep(self.profile.request(bytes)).await;
    }
}

impl Display for RemoteObjectStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "remote({}) {}", self.profile.label(), self.inner)
    }
}

#[derive(Debug)]
struct RemoteMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    profile: RemoteProfile,
}

#[async_trait]
impl MultipartUpload for RemoteMultipartUpload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let cost = self.profile.request(payload.content_length() as u64);
        let inner = self.inner.put_part(payload);
        Box::pin(async move {
            tokio::time::sleep(cost).await;
            inner.await
        })
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        tokio::time::sleep(self.profile.round_trip).await;
        self.inner.complete().await
    }

    async fn abort(&mut self) -> ObjectStoreResult<()> {
        tokio::time::sleep(self.profile.round_trip).await;
        self.inner.abort().await
    }
}

#[async_trait]
impl ObjectStore for RemoteObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.pay(payload.content_length() as u64).await;
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        tokio::time::sleep(self.profile.round_trip).await;
        Ok(Box::new(RemoteMultipartUpload {
            inner: self.inner.put_multipart_opts(location, options).await?,
            profile: self.profile,
        }))
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let result = self.inner.get_opts(location, options).await?;
        // Bill the bytes this request will actually return, without draining
        // the stream: the caller's memory profile must not change.
        let Range { start, end } = result.range.clone();
        self.pay(end.saturating_sub(start)).await;
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        let result = self.inner.get_ranges(location, ranges).await?;
        self.pay(result.iter().map(|bytes| bytes.len() as u64).sum())
            .await;
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
        let round_trip = self.profile.round_trip;
        self.inner
            .delete_stream(locations)
            .then(move |item| async move {
                tokio::time::sleep(round_trip).await;
                item
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        // Listing is a metadata walk; charge the round trip once, on the
        // stream's first item, rather than per returned object.
        let round_trip = self.profile.round_trip;
        let mut charged = false;
        self.inner
            .list(prefix)
            .then(move |item| {
                let pay = !std::mem::replace(&mut charged, true);
                async move {
                    if pay {
                        tokio::time::sleep(round_trip).await;
                    }
                    item
                }
            })
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<ListResult> {
        self.pay(0).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.pay(0).await;
        self.inner.copy_opts(from, to, options).await
    }
}
