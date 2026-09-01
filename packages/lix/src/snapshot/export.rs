use std::ops::Bound;

use futures_io::AsyncWrite;
use futures_lite::io::AsyncWriteExt as _;
use futures_util::StreamExt as _;

use super::format::{SnapshotEncoder, SnapshotEntry};
use crate::storage_adapter::{
    MAX_SCAN_PAGE_ROWS, StorageAdapter, StorageAdapterRead as _, StorageBeginScanOptions,
    StorageCoreProjection, StorageKeyRange, StorageProjectedValue, StorageReadOptions,
    StorageReadDurability as ReadDurability, StorageSession, Storage,
};
use crate::LixError;

/// Summary of a completed snapshot export.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotExportReport {
    pub entry_count: u64,
    pub payload_bytes: u64,
    pub digest: [u8; 32],
}

/// Configures and streams one coherent Lix snapshot.
#[expect(missing_debug_implementations)]
pub struct SnapshotExportBuilder<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    storage: StorageAdapter<StorageSession<StorageImpl>>,
    durability: ReadDurability,
    preflight_error: Option<LixError>,
    remote: Option<RemoteSnapshotExport>,
}

#[derive(Clone)]
struct RemoteSnapshotExport {
    http: crate::sync::AuthorityHttp,
    url: String,
    session_id: Option<String>,
}

impl<StorageImpl> SnapshotExportBuilder<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(storage: StorageAdapter<StorageSession<StorageImpl>>) -> Self {
        Self {
            storage,
            durability: ReadDurability::Visible,
            preflight_error: None,
            remote: None,
        }
    }

    pub(crate) fn from_connected_authority(
        mut self,
        http: crate::sync::AuthorityHttp,
        url: Result<String, LixError>,
        session_id: Option<String>,
    ) -> Self {
        match url {
            Ok(url) => self.remote = Some(RemoteSnapshotExport { http, url, session_id }),
            Err(error) => self.preflight_error = Some(error),
        }
        self
    }

    pub(crate) fn reject_connected_replica(mut self) -> Self {
        self.preflight_error = Some(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "a connected replica is a sparse cache and cannot export a canonical repository snapshot; export from the authority",
        ));
        self
    }

    /// Selects the minimum persistence boundary of the source read.
    pub fn durability(mut self, durability: ReadDurability) -> Self {
        self.durability = durability;
        self
    }

    /// Writes the snapshot without buffering the complete artifact in memory.
    ///
    /// If the source read expires or the sink fails, the partial output is not
    /// a valid snapshot and must be discarded.
    pub async fn write_to<W>(self, writer: &mut W) -> Result<SnapshotExportReport, LixError>
    where
        W: AsyncWrite + Unpin + Send + ?Sized,
    {
        if let Some(error) = self.preflight_error {
            return Err(error);
        }
        if let Some(remote) = self.remote {
            return remote.write_to(writer, self.durability).await;
        }
        let read = self
            .storage
            .begin_read(StorageReadOptions {
                durability: self.durability,
                ..StorageReadOptions::default()
            })
            .await?;
        if crate::sync::has_any_sync_replica_state(&read).await? {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "a persisted replica is a sparse cache and cannot export a canonical repository snapshot; export from the authority",
            ));
        }
        let mut encoder =
            SnapshotEncoder::new(writer, crate::init::CURRENT_FORMAT_VERSION).await?;
        for space in super::snapshot_spaces() {
            let mut cursor = read
                .begin_scan(
                    space,
                    StorageKeyRange {
                        lower: Bound::Unbounded,
                        upper: Bound::Unbounded,
                    },
                    StorageBeginScanOptions {
                        projection: StorageCoreProjection::FullValue,
                        ..StorageBeginScanOptions::default()
                    },
                )
                .await?;
            loop {
                let (entries, has_more) = cursor
                    .next_page(MAX_SCAN_PAGE_ROWS)
                    .await?
                    .into_parts();
                for entry in entries {
                    // Authority admission is a host-local write fence, not
                    // repository data. Exporting it would make a restored
                    // standalone snapshot permanently reject ordinary writes.
                    if space == crate::sync::SYNC_AUTHORITY_STATE_SPACE
                        && entry.key == crate::sync::authority_state_key()
                    {
                        continue;
                    }
                    let StorageProjectedValue::FullValue(value) = entry.value else {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "full-value snapshot scan returned a key-only entry",
                        ));
                    };
                    encoder
                        .write_entry(&SnapshotEntry {
                            space_id: space.id.0,
                            key: entry.key.0,
                            value,
                        })
                        .await?;
                }
                if !has_more {
                    break;
                }
            }
        }
        let trailer = encoder.finish().await?;
        Ok(SnapshotExportReport {
            entry_count: trailer.entry_count,
            payload_bytes: trailer.payload_bytes,
            digest: trailer.digest,
        })
    }
}

impl RemoteSnapshotExport {
    async fn write_to<W>(
        self,
        writer: &mut W,
        durability: ReadDurability,
    ) -> Result<SnapshotExportReport, LixError>
    where
        W: AsyncWrite + Unpin + Send + ?Sized,
    {
        use crate::authority_client::{ProtocolHttp as _, ProtocolHttpRequest};
        let mut headers = vec![
            ("accept".to_owned(), "application/vnd.lix.snapshot".to_owned()),
            (
                "lix-snapshot-durability".to_owned(),
                authority_snapshot_durability(durability).to_owned(),
            ),
        ];
        if let Some(session_id) = self.session_id {
            headers.push(("lix-session-id".to_owned(), session_id));
        }
        let response = self
            .http
            .request_stream(ProtocolHttpRequest {
                method: "GET".to_owned(),
                url: self.url,
                headers,
                body: None,
            })
            .await?;
        if !(200..300).contains(&response.status) {
            return Err(LixError::new(
                "LIX_REMOTE_REQUEST_FAILED",
                format!("authority snapshot request failed with HTTP {}", response.status),
            ));
        }

        write_verified_snapshot_stream(response.body, writer).await
    }
}

fn authority_snapshot_durability(durability: ReadDurability) -> &'static str {
    match durability {
        ReadDurability::Visible => "visible",
        ReadDurability::Durable => "durable",
    }
}

async fn write_verified_snapshot_stream<S, W>(
    mut stream: S,
    writer: &mut W,
) -> Result<SnapshotExportReport, LixError>
where
    S: futures_core::Stream<Item = Result<bytes::Bytes, LixError>> + Unpin,
    W: AsyncWrite + Unpin + Send + ?Sized,
{
    let mut total_bytes = 0_u64;
    let mut header = [0_u8; super::format::HEADER_BYTES];
    let mut header_bytes = 0_usize;
    let mut tail = Vec::with_capacity(super::format::TRAILER_BYTES);
    let mut hasher = blake3::Hasher::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        total_bytes = total_bytes
            .checked_add(u64::try_from(chunk.len()).map_err(|_| {
                LixError::new(LixError::CODE_INVALID_PARAM, "authority snapshot is too large")
            })?)
            .ok_or_else(|| {
                LixError::new(LixError::CODE_INVALID_PARAM, "authority snapshot is too large")
            })?;

        let mut remaining = chunk.as_ref();
        if header_bytes < header.len() {
            let take = remaining.len().min(header.len() - header_bytes);
            header[header_bytes..header_bytes + take].copy_from_slice(&remaining[..take]);
            header_bytes += take;
            remaining = &remaining[take..];
            if header_bytes == header.len() {
                super::format::decode_streamed_snapshot_header(&header)?;
                writer
                    .write_all(&header)
                    .await
                    .map_err(authority_snapshot_io_error)?;
            }
        }
        if header_bytes == header.len() && !remaining.is_empty() {
            writer
                .write_all(remaining)
                .await
                .map_err(authority_snapshot_io_error)?;
            retain_trailer_and_hash_payload(&mut hasher, &mut tail, remaining);
        }
    }
    if header_bytes != header.len() {
        return Err(super::format::invalid_snapshot(
            "authority snapshot is truncated in its header",
        ));
    }
    let trailer = super::format::decode_streamed_snapshot_trailer(
        total_bytes,
        *hasher.finalize().as_bytes(),
        &tail,
    )?;
    writer.flush().await.map_err(authority_snapshot_io_error)?;
    Ok(SnapshotExportReport {
        entry_count: trailer.entry_count,
        payload_bytes: trailer.payload_bytes,
        digest: trailer.digest,
    })
}

fn retain_trailer_and_hash_payload(
    hasher: &mut blake3::Hasher,
    tail: &mut Vec<u8>,
    bytes: &[u8],
) {
    let finalized = tail
        .len()
        .saturating_add(bytes.len())
        .saturating_sub(super::format::TRAILER_BYTES);
    let from_tail = finalized.min(tail.len());
    hasher.update(&tail[..from_tail]);
    tail.drain(..from_tail);
    let from_bytes = finalized - from_tail;
    hasher.update(&bytes[..from_bytes]);
    tail.extend_from_slice(&bytes[from_bytes..]);
    debug_assert!(tail.len() <= super::format::TRAILER_BYTES);
}

fn authority_snapshot_io_error(error: std::io::Error) -> LixError {
    LixError::new(
        LixError::CODE_UNKNOWN,
        format!("write authority snapshot: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_lite::stream;

    use super::*;

    #[tokio::test]
    async fn verified_remote_snapshot_stream_preserves_chunked_bytes_and_report() {
        let mut valid = Vec::new();
        let trailer = SnapshotEncoder::new(&mut valid, 76)
            .await
            .expect("encode header")
            .finish()
            .await
            .expect("encode trailer");
        let chunks = valid
            .chunks(3)
            .map(Bytes::copy_from_slice)
            .map(Ok)
            .collect::<Vec<Result<Bytes, LixError>>>();
        let mut forwarded = Vec::new();

        let report = write_verified_snapshot_stream(stream::iter(chunks), &mut forwarded)
            .await
            .expect("valid authority snapshot");

        assert_eq!(forwarded, valid);
        assert_eq!(report.entry_count, trailer.entry_count);
        assert_eq!(report.payload_bytes, trailer.payload_bytes);
        assert_eq!(report.digest, trailer.digest);
    }

    #[tokio::test]
    async fn corrupt_remote_headers_are_rejected_before_any_bytes_are_forwarded() {
        let mut valid = Vec::new();
        SnapshotEncoder::new(&mut valid, 76)
            .await
            .expect("encode header")
            .finish()
            .await
            .expect("encode trailer");

        for (label, offset, value) in [
            ("magic", 0, b'X'),
            (
                "container version",
                crate::snapshot::format::MAGIC.len() + 1,
                2,
            ),
            (
                "checksum",
                crate::snapshot::format::MAGIC.len() + 2,
                0xff,
            ),
            (
                "reserved flags",
                crate::snapshot::format::MAGIC.len() + 3,
                1,
            ),
        ] {
            let mut corrupt = valid.clone();
            corrupt[offset] = value;
            let chunks = stream::iter([Ok(Bytes::from(corrupt))]);
            let mut forwarded = Vec::new();

            let error = write_verified_snapshot_stream(chunks, &mut forwarded)
                .await
                .expect_err("corrupt authority header must fail");

            assert_eq!(error.code, LixError::CODE_INVALID_SNAPSHOT, "{label}");
            assert!(forwarded.is_empty(), "{label}");
        }
    }

    #[test]
    fn remote_snapshot_durability_is_preserved_on_the_authority_request() {
        assert_eq!(
            authority_snapshot_durability(ReadDurability::Visible),
            "visible"
        );
        assert_eq!(
            authority_snapshot_durability(ReadDurability::Durable),
            "durable"
        );
    }
}
