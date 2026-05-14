use crate::backend_v2::{
    BackendError, BackendRead, Key, Prefix, ReadBatch, ScanOptions, ScanPage, SpaceId,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScanResumeKey {
    pub last_key: Option<Key>,
}

impl ScanResumeKey {
    pub fn start() -> Self {
        Self { last_key: None }
    }

    pub fn from_last_key(last_key: Key) -> Self {
        Self {
            last_key: Some(last_key),
        }
    }
}

pub(crate) fn scan_prefix<R>(
    read: &R,
    space: SpaceId,
    prefix: Prefix,
    opts: ScanOptions<'_>,
) -> Result<ScanPage, BackendError>
where
    R: BackendRead,
{
    if opts.limit_rows == 0 {
        return Ok(ScanPage {
            entries: ReadBatch::default(),
            has_more: false,
        });
    }
    read.scan_range(space, prefix.to_range()?, opts)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::backend_v2::{
        ConformanceBackend, Key, Prefix, ReadOptions, ScanOptions, SpaceId, StoredValue,
        WriteOptions,
    };
    use crate::storage_v2::{StorageContext, StorageReader, StorageSpace};

    fn key(bytes: &'static str) -> Key {
        Key(Bytes::from_static(bytes.as_bytes()))
    }

    fn value(bytes: &'static str) -> StoredValue {
        StoredValue {
            bytes: Bytes::from_static(bytes.as_bytes()),
        }
    }

    fn space(id: u32) -> StorageSpace {
        StorageSpace::new(SpaceId(id))
    }

    #[test]
    fn prefix_scan_limit_zero_returns_empty_page() {
        let storage = StorageContext::new(ConformanceBackend::new());
        let mut writes = storage.new_write_set();
        writes.stage_put(space(1), key("aa"), value("AA"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");

        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let page = read
            .scan_prefix(
                space(1),
                Prefix {
                    bytes: Bytes::from_static(b"a"),
                },
                ScanOptions {
                    limit_rows: 0,
                    ..ScanOptions::default()
                },
            )
            .expect("prefix scan");

        assert!(page.entries.entries.is_empty());
        assert!(!page.has_more);
    }
}
