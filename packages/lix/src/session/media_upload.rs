use crate::Blob;
use crate::forktree::UploadBindingRef;
use crate::storage_adapter::Storage;
use crate::{LixError, common::LixPath};

use super::SessionContext;

pub const FILE_UPLOAD_PART_BYTES: usize = 16 * 1024 * 1024;
const MAX_FILE_UPLOAD_BYTES: u64 = 20 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileUploadProgress {
    pub next_offset: u64,
    pub total_size: u64,
    pub finalized: bool,
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Publishes one authenticated upload part through the transaction-owned
    /// ForkTree publication plan. The upload view, typed receipt objects,
    /// completed manifest, and visible file row all reach the existing single
    /// prepare/commit boundary; this method never opens an upload-specific
    /// write set or invokes a Binary CAS writer.
    pub async fn upsert_file_content_part(
        &self,
        upload_id: String,
        path: String,
        start: u64,
        total_size: u64,
        content: Blob,
    ) -> Result<FileUploadProgress, LixError> {
        self.ensure_open()?;
        LixPath::try_from_file_path(&path)?;
        validate_upload_request(&upload_id, start, total_size, content.len())?;
        let operation_guard = self.begin_waitable_session_operation().await?;
        let part_number = start / FILE_UPLOAD_PART_BYTES as u64;
        let repository_identity = self.active_account_id.as_bytes().to_vec();
        let path_bytes = path.as_bytes().to_vec();
        let payload_domain = b"lix-file-content".to_vec();
        drop(operation_guard);
        let write_access = self.begin_session_write_access().await?;
        let result = self
            .with_write_transaction_reserved_lending(
                write_access,
                async move |transaction| {
                    let binding = UploadBindingRef {
                        repository_identity: &repository_identity,
                        path: &path_bytes,
                        payload_domain: &payload_domain,
                        declared_total_size: total_size,
                        declared_final_hash: None,
                    };
                    if transaction
                        .check_forktree_upload_target(
                            &path,
                            total_size,
                            start,
                            content.as_ref(),
                        )
                        .await?
                    {
                        return Ok(FileUploadProgress {
                            next_offset: total_size,
                            total_size,
                            finalized: true,
                        });
                    }
                    let prepared = transaction
                        .stage_forktree_upload_part(
                            &upload_id,
                            binding,
                            part_number,
                            start,
                            content.as_ref(),
                        )
                        .await?;
                    let finalized = prepared.complete_receipt.is_some();
                    if let Some(receipt) = prepared.complete_receipt.clone() {
                        crate::sql2::execute_fast_lix_file_prepared_path_write(
                            transaction,
                            path,
                            receipt,
                        )
                        .await?
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_CONSTRAINT_VIOLATION,
                                "resumable file publication requires an unambiguous filesystem layout",
                            )
                        })?;
                    }
                    Ok(FileUploadProgress {
                        next_offset: prepared.progress.contiguous_prefix_bytes,
                        total_size,
                        finalized,
                    })
                },
                |_| Ok(()),
            )
            .await;
        result
    }
}

fn validate_upload_request(
    upload_id: &str,
    start: u64,
    total_size: u64,
    part_len: usize,
) -> Result<(), LixError> {
    if upload_id.is_empty() || upload_id.len() > 200 || !upload_id.is_ascii() {
        return Err(invalid_upload("upload id must be 1-200 ASCII bytes"));
    }
    if total_size > MAX_FILE_UPLOAD_BYTES {
        return Err(invalid_upload("file exceeds the 20 GiB media target"));
    }
    let end = start
        .checked_add(part_len as u64)
        .ok_or_else(|| invalid_upload("upload range exceeds u64"))?;
    if end > total_size || part_len > FILE_UPLOAD_PART_BYTES {
        return Err(invalid_upload(
            "upload part is outside the declared file size",
        ));
    }
    if end < total_size && part_len != FILE_UPLOAD_PART_BYTES {
        return Err(invalid_upload(
            "non-final upload parts must be exactly 16 MiB",
        ));
    }
    if start % FILE_UPLOAD_PART_BYTES as u64 != 0 {
        return Err(invalid_upload("upload part offset must be 16 MiB aligned"));
    }
    Ok(())
}

#[cfg(test)]
fn validate_upload_binding(
    existing_path: &str,
    existing_total_size: u64,
    path: &str,
    total_size: u64,
) -> Result<(), LixError> {
    if existing_path != path || existing_total_size != total_size {
        return Err(invalid_upload(
            "upload id is already bound to a different path or size",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn upload_part_count(total_size: u64) -> Result<u32, LixError> {
    if total_size == 0 {
        return Ok(1);
    }
    let parts = total_size.div_ceil(FILE_UPLOAD_PART_BYTES as u64);
    u32::try_from(parts).map_err(|_| invalid_upload("upload part count exceeds u32"))
}

#[cfg(test)]
fn upload_part_size(total_size: u64, part_number: u32) -> Result<u64, LixError> {
    if total_size == 0 && part_number == 0 {
        return Ok(0);
    }
    let start = u64::from(part_number)
        .checked_mul(FILE_UPLOAD_PART_BYTES as u64)
        .ok_or_else(|| invalid_upload("upload part offset exceeds u64"))?;
    if start >= total_size {
        return Err(invalid_upload(
            "upload part is outside the declared file size",
        ));
    }
    Ok((total_size - start).min(FILE_UPLOAD_PART_BYTES as u64))
}

fn invalid_upload(message: &'static str) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use crate::storage::Memory;

    #[tokio::test]
    async fn public_upload_race_preserves_one_authenticated_winner_and_reopens() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("test repository should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("test repository should open");
        let left = engine
            .open_workspace_session()
            .await
            .expect("left upload session should open");
        let right = engine
            .open_workspace_session()
            .await
            .expect("right upload session should open");
        let total_size = FILE_UPLOAD_PART_BYTES as u64;
        let left_bytes = vec![0x31; FILE_UPLOAD_PART_BYTES];
        let right_bytes = vec![0x72; FILE_UPLOAD_PART_BYTES];
        let (left_result, right_result) = tokio::join!(
            left.upsert_file_content_part(
                "receipt-race".to_owned(),
                "/media/receipt-race.bin".to_owned(),
                0,
                total_size,
                left_bytes.clone().into()
            ),
            right.upsert_file_content_part(
                "receipt-race".to_owned(),
                "/media/receipt-race.bin".to_owned(),
                0,
                total_size,
                right_bytes.clone().into()
            ),
        );
        assert!(
            left_result.is_ok() ^ right_result.is_ok(),
            "one receipt publication must win: left={left_result:?} right={right_result:?}"
        );
        let expected = if left_result.is_ok() {
            left_bytes
        } else {
            right_bytes
        };
        let reopened = Engine::new(storage)
            .await
            .expect("published upload should cold reopen");
        let session = reopened
            .open_workspace_session()
            .await
            .expect("reopened upload session should open");
        let read = session
            .read_file_content("/media/receipt-race.bin".to_owned(), None)
            .await
            .expect("published upload should read");
        assert_eq!(
            read.expect("winner file should exist")
                .into_content()
                .as_ref(),
            expected.as_slice()
        );
        let replay = session
            .upsert_file_content_part(
                "receipt-race".to_owned(),
                "/media/receipt-race.bin".to_owned(),
                0,
                total_size,
                expected.clone().into(),
            )
            .await
            .expect("identical completed-part replay should be idempotent");
        assert!(replay.finalized);
        let mismatch = session
            .upsert_file_content_part(
                "receipt-race".to_owned(),
                "/media/receipt-race.bin".to_owned(),
                0,
                total_size,
                vec![0xa5; FILE_UPLOAD_PART_BYTES].into(),
            )
            .await
            .expect_err("conflicting completed-part replay must fail closed");
        assert_eq!(mismatch.code, LixError::CODE_INVALID_PARAM);
    }

    #[tokio::test]
    async fn public_multipart_upload_reopens_and_preserves_shared_final_references() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("test repository should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("test repository should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("upload session should open");
        let part_size = FILE_UPLOAD_PART_BYTES as u64;
        let total_size = part_size * 2 + 7;
        let first = vec![0x11; FILE_UPLOAD_PART_BYTES];
        let second = vec![0x22; FILE_UPLOAD_PART_BYTES];
        let tail = vec![0x33; 7];
        assert!(
            !session
                .upsert_file_content_part(
                    "multipart-a".to_owned(),
                    "/media/multipart-a.bin".to_owned(),
                    0,
                    total_size,
                    first.clone().into()
                )
                .await
                .expect("first multipart part should publish")
                .finalized
        );
        assert!(
            !session
                .upsert_file_content_part(
                    "multipart-a".to_owned(),
                    "/media/multipart-a.bin".to_owned(),
                    part_size,
                    total_size,
                    second.clone().into()
                )
                .await
                .expect("second multipart part should publish")
                .finalized
        );
        assert!(
            session
                .upsert_file_content_part(
                    "multipart-a".to_owned(),
                    "/media/multipart-a.bin".to_owned(),
                    part_size * 2,
                    total_size,
                    tail.clone().into()
                )
                .await
                .expect("final multipart part should publish")
                .finalized
        );
        let mut expected = first;
        expected.extend_from_slice(&second);
        expected.extend_from_slice(&tail);
        let content = session
            .read_file_content("/media/multipart-a.bin".to_owned(), None)
            .await
            .expect("published multipart file should read")
            .expect("published multipart file should exist")
            .into_content();
        assert_eq!(content.as_ref(), expected.as_slice());
        let reopened = Engine::new(storage.clone())
            .await
            .expect("multipart repository should cold reopen");
        let reopened_session = reopened
            .open_workspace_session()
            .await
            .expect("reopened upload session should open");
        let reopened_content = reopened_session
            .read_file_content("/media/multipart-a.bin".to_owned(), None)
            .await
            .expect("reopened multipart file should read")
            .expect("reopened multipart file should exist")
            .into_content();
        assert_eq!(reopened_content.as_ref(), expected.as_slice());
        reopened_session
            .execute(
                "DELETE FROM lix_file WHERE path = $1",
                &[crate::Value::Text("/media/multipart-a.bin".to_owned())],
            )
            .await
            .expect("final file reference deletion should publish");
        assert!(
            reopened_session
                .read_file_content("/media/multipart-a.bin".to_owned(), None)
                .await
                .expect("deleted final reference should be readable as absence")
                .is_none()
        );
    }

    #[tokio::test]
    async fn public_upload_rejects_active_receipt_size_conflict() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("test repository should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("test repository should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("upload session should open");
        let part = vec![0x44; FILE_UPLOAD_PART_BYTES];
        session
            .upsert_file_content_part(
                "active-size-conflict".to_owned(),
                "/media/active-size-conflict.bin".to_owned(),
                0,
                (FILE_UPLOAD_PART_BYTES * 2) as u64,
                part.clone().into(),
            )
            .await
            .expect("initial active receipt should publish");
        let error = session
            .upsert_file_content_part(
                "active-size-conflict".to_owned(),
                "/media/active-size-conflict.bin".to_owned(),
                0,
                (FILE_UPLOAD_PART_BYTES * 3) as u64,
                part.into(),
            )
            .await
            .expect_err("active receipt size conflict must fail closed");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }

    #[tokio::test]
    async fn public_shared_chunk_survives_first_reference_and_releases_after_last() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("test repository should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("test repository should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("upload session should open");
        let shared = vec![0x55; FILE_UPLOAD_PART_BYTES];
        for (upload_id, path) in [
            ("shared-a", "/media/shared-a.bin"),
            ("shared-b", "/media/shared-b.bin"),
        ] {
            session
                .upsert_file_content_part(
                    upload_id.to_owned(),
                    path.to_owned(),
                    0,
                    FILE_UPLOAD_PART_BYTES as u64,
                    shared.clone().into(),
                )
                .await
                .expect("shared chunk upload should finalize");
        }
        assert_eq!(
            session
                .read_file_content("/media/shared-a.bin".to_owned(), None)
                .await
                .expect("first shared reference should read")
                .expect("first shared reference should exist")
                .into_content()
                .as_ref(),
            shared.as_slice()
        );
        session
            .execute(
                "DELETE FROM lix_file WHERE path = $1",
                &[crate::Value::Text("/media/shared-a.bin".to_owned())],
            )
            .await
            .expect("first shared reference deletion should publish");
        assert_eq!(
            session
                .read_file_content("/media/shared-b.bin".to_owned(), None)
                .await
                .expect("shared chunk must survive first reference release")
                .expect("second shared reference should exist")
                .into_content()
                .as_ref(),
            shared.as_slice()
        );
        session
            .execute(
                "DELETE FROM lix_file WHERE path = $1",
                &[crate::Value::Text("/media/shared-b.bin".to_owned())],
            )
            .await
            .expect("last shared reference deletion should publish");
        assert!(
            session
                .read_file_content("/media/shared-b.bin".to_owned(), None)
                .await
                .expect("released final reference should be readable as absence")
                .is_none()
        );
    }

    #[test]
    fn upload_validation_keeps_receipt_window_and_identity_guards() {
        assert!(
            validate_upload_request(
                "upload",
                0,
                FILE_UPLOAD_PART_BYTES as u64,
                FILE_UPLOAD_PART_BYTES
            )
            .is_ok()
        );
        assert!(
            validate_upload_request(
                "upload",
                1,
                FILE_UPLOAD_PART_BYTES as u64,
                FILE_UPLOAD_PART_BYTES
            )
            .is_err()
        );
        assert!(validate_upload_binding("/a", 4, "/a", 4).is_ok());
        assert!(validate_upload_binding("/a", 4, "/b", 4).is_err());
        assert_eq!(
            upload_part_count(FILE_UPLOAD_PART_BYTES as u64 * 2).unwrap(),
            2
        );
        assert_eq!(
            upload_part_size(FILE_UPLOAD_PART_BYTES as u64 + 7, 1).unwrap(),
            7
        );
    }
}
