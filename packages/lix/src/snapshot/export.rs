use std::ops::Bound;

use futures_io::AsyncWrite;

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
}

impl<StorageImpl> SnapshotExportBuilder<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(storage: StorageAdapter<StorageSession<StorageImpl>>) -> Self {
        Self {
            storage,
            durability: ReadDurability::Visible,
        }
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
        let read = self
            .storage
            .begin_read(StorageReadOptions {
                durability: self.durability,
                ..StorageReadOptions::default()
            })
            .await?;
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
