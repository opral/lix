mod export;
pub(crate) mod format;
mod restore;

pub use export::{SnapshotExportBuilder, SnapshotExportReport};
pub(crate) use restore::restore_snapshot;

use crate::storage_adapter::{REPOSITORY_EPOCH_SPACE, StorageSpace};

pub(crate) fn snapshot_spaces() -> impl Iterator<Item = StorageSpace> {
    crate::storage_spaces::ALL_STORAGE_SPACES
        .iter()
        .copied()
        .filter(|space| space.id != REPOSITORY_EPOCH_SPACE.id)
}

pub(crate) fn snapshot_space(space_id: u32) -> Option<StorageSpace> {
    crate::storage_spaces::SNAPSHOT_STORAGE_SPACES
        .iter()
        .copied()
        .find(|space| space.id.0 == space_id)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_lite::io::Cursor;

    use crate::storage_adapter::{
        StorageAdapter, StorageKey, StorageValue, StorageWriteOptions,
    };
    use crate::{Memory, open_lix};

    #[test]
    fn registered_snapshot_spaces_are_canonical_and_cover_the_active_layout() {
        let ids = super::snapshot_spaces()
            .map(|space| space.id.0)
            .collect::<Vec<_>>();
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));

        let wire = crate::storage_spaces::SNAPSHOT_STORAGE_SPACES;
        assert!(wire.windows(2).all(|pair| pair[0].id.0 < pair[1].id.0));
        assert!(wire
            .iter()
            .all(|space| space.id != crate::storage_adapter::REPOSITORY_EPOCH_SPACE.id));
        for active in super::snapshot_spaces() {
            assert_eq!(super::snapshot_space(active.id.0), Some(active));
        }
    }

    #[tokio::test]
    async fn complete_lix_roundtrip_is_byte_identical() {
        let source = open_lix().await.expect("open source Lix");
        source
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('snapshot-test', 'complete')",
                &[],
            )
            .await
            .expect("seed snapshot state");
        source
            .create_checkpoint()
            .await
            .expect("checkpoint snapshot state");

        let mut bytes = Vec::new();
        let report = source
            .export_snapshot()
            .write_to(&mut bytes)
            .await
            .expect("export snapshot");
        assert!(report.entry_count > 0);
        assert!(bytes.starts_with(b"LIXSNAP\0"));

        let restored = open_lix()
            .from_snapshot(Cursor::new(bytes.clone()))
            .await
            .expect("restore snapshot");
        let result = restored
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'snapshot-test'",
                &[],
            )
            .await
            .expect("query restored state");
        assert_eq!(result.rows().len(), 1);

        let mut roundtrip = Vec::new();
        restored
            .export_snapshot()
            .write_to(&mut roundtrip)
            .await
            .expect("re-export snapshot");
        assert_eq!(roundtrip, bytes);
    }

    #[cfg(feature = "server-protocol")]
    #[tokio::test]
    async fn protocol_owner_exports_without_opening_a_second_engine() {
        let storage = Memory::new();
        let source = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("open source Lix");
        source
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('protocol-snapshot', 'complete')",
                &[],
            )
            .await
            .expect("seed snapshot state");
        source.close().await.expect("close source Lix");

        let server = open_lix()
            .with_storage(storage)
            .serve().with_embedded_lix_id()
            .await
            .expect("serve source Lix");
        let mut bytes = Vec::new();
        server
            .export_snapshot()
            .write_to(&mut bytes)
            .await
            .expect("export from protocol owner");

        let restored = open_lix()
            .from_snapshot(Cursor::new(bytes))
            .await
            .expect("restore protocol snapshot");
        let result = restored
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'protocol-snapshot'",
                &[],
            )
            .await
            .expect("query restored state");
        assert_eq!(result.rows().len(), 1);

        restored.close().await.expect("close restored Lix");
        server.close().await.expect("close protocol server");
    }

    #[tokio::test]
    async fn restore_rejects_nonempty_storage_without_changing_it() {
        let mut bytes = Vec::new();
        let source = open_lix().await.expect("open source Lix");
        source
            .export_snapshot()
            .write_to(&mut bytes)
            .await
            .expect("export source snapshot");

        let storage = Memory::new();
        let existing = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("open destination seed");
        existing.close().await.expect("close destination seed");
        let error = match open_lix()
            .with_storage(storage)
            .from_snapshot(Cursor::new(bytes))
            .await
        {
            Ok(_) => panic!("nonempty destination was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.code, crate::LixError::CODE_INVALID_PARAM);
    }

    #[tokio::test]
    async fn restore_rejects_storage_with_only_retired_lix_rows() {
        let mut bytes = Vec::new();
        open_lix()
            .await
            .expect("open snapshot source")
            .export_snapshot()
            .write_to(&mut bytes)
            .await
            .expect("export source snapshot");

        let storage = Memory::new();
        let adapter = StorageAdapter::new(storage.clone());
        let retired = crate::storage_spaces::RETIRED_STORAGE_SPACES[0].space;
        let mut writes = adapter.new_write_set();
        writes.put(
            retired,
            StorageKey(Bytes::from_static(b"retired-key")),
            StorageValue {
                bytes: Bytes::from_static(b"retired-value"),
            },
        );
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("seed retired row");

        let error = match open_lix()
            .with_storage(storage)
            .from_snapshot(Cursor::new(bytes))
            .await
        {
            Ok(_) => panic!("retired Lix-owned rows made a destination appear empty"),
            Err(error) => error,
        };
        assert_eq!(error.code, crate::LixError::CODE_INVALID_PARAM);
    }

    #[tokio::test]
    async fn failed_restore_cleans_candidate_and_destination_is_reusable() {
        let source = open_lix().await.expect("open source Lix");
        source
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('cleanup-test', 'complete')",
                &[],
            )
            .await
            .expect("seed cleanup test");
        let mut valid = Vec::new();
        source
            .export_snapshot()
            .write_to(&mut valid)
            .await
            .expect("export valid snapshot");
        let mut corrupt = valid.clone();
        let digest_byte = corrupt
            .len()
            .checked_sub(16)
            .expect("snapshot has a trailer digest");
        corrupt[digest_byte] ^= 1;

        let storage = Memory::new();
        let error = match open_lix()
            .with_storage(storage.clone())
            .from_snapshot(Cursor::new(corrupt))
            .await
        {
            Ok(_) => panic!("corrupt snapshot was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.code, crate::LixError::CODE_INVALID_SNAPSHOT);

        let restored = open_lix()
            .with_storage(storage)
            .from_snapshot(Cursor::new(valid))
            .await
            .expect("failed restore should leave destination reusable");
        let result = restored
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'cleanup-test'",
                &[],
            )
            .await
            .expect("reused destination contains restored state");
        assert_eq!(result.rows().len(), 1);
    }
}
