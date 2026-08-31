//! Durable admission for the one-time sync replica bootstrap.

use crate::storage_adapter::{
    Storage, StorageAdapter, StorageReadDurability, StorageReadOptions,
};
use crate::{Lix, LixError};

use super::platform::HttpSyncTransport;
use super::repository::{InitialSyncSnapshotInstall, SyncReplicaBinding};
use super::{SyncTransport, runtime};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SyncBootstrapAdmission {
    Prepare,
    Ready { account_id: String },
}

#[derive(Debug)]
pub(crate) struct PreparedSyncBootstrap {
    transport: HttpSyncTransport,
    snapshot: runtime::PreparedRepositorySnapshot,
    lix_id: String,
    pub(crate) default_branch_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum BootstrapTier {
    Empty,
    Unbound,
    Bound { account_id: String },
    Ambiguous,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum BootstrapInspection {
    Prepare,
    Publishing,
    Ready { account_id: String },
    Ambiguous,
}

pub(crate) async fn inspect_sync_bootstrap<StorageImpl>(
    storage: &StorageImpl,
    remote_id: &str,
) -> Result<SyncBootstrapAdmission, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let adapter = StorageAdapter::new(storage.clone());
    inspect_sync_bootstrap_with_adapter(&adapter, remote_id).await
}

pub(crate) async fn inspect_sync_bootstrap_with_adapter<StorageImpl>(
    adapter: &StorageAdapter<StorageImpl>,
    remote_id: &str,
) -> Result<SyncBootstrapAdmission, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    super::repository::migrate_legacy_sync_replica_state(adapter).await?;
    match inspect_once(&adapter, remote_id).await? {
        BootstrapInspection::Prepare => Ok(SyncBootstrapAdmission::Prepare),
        BootstrapInspection::Ready { account_id } => {
            Ok(SyncBootstrapAdmission::Ready { account_id })
        }
        BootstrapInspection::Publishing => Err(LixError::new(
            LixError::CODE_STORAGE_READ_EXPIRED,
            "sync bootstrap publication is not durable yet",
        )
        .with_details(serde_json::json!({ "retryable": true }))),
        BootstrapInspection::Ambiguous => Err(ambiguous_replica_error()),
    }
}

async fn inspect_once<StorageImpl>(
    adapter: &StorageAdapter<StorageImpl>,
    remote_id: &str,
) -> Result<BootstrapInspection, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let durable = adapter
        .begin_read(StorageReadOptions {
            durability: StorageReadDurability::Durable,
            ..StorageReadOptions::default()
        })
        .await?;
    let durable_tier = inspect_tier(&durable, remote_id).await?;
    match durable_tier {
        BootstrapTier::Ambiguous => return Ok(BootstrapInspection::Ambiguous),
        BootstrapTier::Empty | BootstrapTier::Unbound | BootstrapTier::Bound { .. } => {}
    }

    let visible = adapter.begin_read(StorageReadOptions::default()).await?;
    let visible_tier = inspect_tier(&visible, remote_id).await?;
    Ok(match (durable_tier, visible_tier) {
        (BootstrapTier::Bound { account_id }, BootstrapTier::Bound { account_id: visible })
            if account_id == visible =>
        {
            BootstrapInspection::Ready { account_id }
        }
        (BootstrapTier::Empty, BootstrapTier::Empty)
        | (BootstrapTier::Unbound, BootstrapTier::Unbound) => BootstrapInspection::Prepare,
        (_, BootstrapTier::Bound { .. } | BootstrapTier::Ambiguous)
        | (BootstrapTier::Empty, BootstrapTier::Unbound)
        | (BootstrapTier::Unbound, BootstrapTier::Empty) => BootstrapInspection::Publishing,
        (BootstrapTier::Bound { .. }, _) => BootstrapInspection::Publishing,
        (BootstrapTier::Ambiguous, _) => unreachable!("terminal durable tier returned above"),
    })
}

async fn inspect_tier(
    read: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
    _remote_id: &str,
) -> Result<BootstrapTier, LixError> {
    match crate::init::repository_protocol_status(read).await? {
        crate::init::RepositoryProtocolStatus::Missing => Ok(BootstrapTier::Empty),
        crate::init::RepositoryProtocolStatus::Current => {
            Ok(match super::repository::inspect_sync_replica_binding(read).await? {
                SyncReplicaBinding::Unbound => BootstrapTier::Unbound,
                SyncReplicaBinding::Bound { account_id } => {
                    BootstrapTier::Bound { account_id }
                }
                SyncReplicaBinding::Ambiguous => BootstrapTier::Ambiguous,
            })
        }
        crate::init::RepositoryProtocolStatus::MigrationRequired { found_version } => {
            Err(crate::init::migration_required_error(found_version))
        }
        crate::init::RepositoryProtocolStatus::TooNew { .. }
        | crate::init::RepositoryProtocolStatus::Malformed => {
            Err(crate::init::unsupported_repository_protocol_error())
        }
    }
}

pub(crate) async fn prepare_sync_bootstrap(
    server: &crate::ServerOptions,
) -> Result<PreparedSyncBootstrap, LixError> {
    let remote_id = server.url.as_str();
    let transport = HttpSyncTransport::connect(remote_id, &server.headers).await?;
    let (snapshot, lix_id, default_branch_id) =
        runtime::fetch_repository_snapshot(&transport).await?;
    if transport.lix_id() != lix_id {
        return Err(super::sync_repository_id_mismatch(
            &lix_id,
            transport.lix_id(),
        ));
    }
    Ok(PreparedSyncBootstrap {
        transport,
        snapshot,
        lix_id,
        default_branch_id,
    })
}

pub(crate) async fn install_sync_bootstrap<StorageImpl>(
    lix: &mut Lix<StorageImpl>,
    server: &crate::ServerOptions,
    prepared: PreparedSyncBootstrap,
) -> Result<HttpSyncTransport, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    if let Err(error) = runtime::register_blob_manifests(
        lix,
        &prepared.transport,
        &prepared.snapshot.commits,
        &prepared.snapshot.rows,
    )
    .await
    {
        return Err(if is_ambiguous_bootstrap_write(&error) {
            restart_open_error()
        } else {
            error
        });
    }
    let install = lix
        .try_install_initial_sync_snapshot(
            &server.url,
            prepared.transport.active_account_id(),
            &prepared.snapshot.metadata,
            &prepared.snapshot.commits,
            &prepared.snapshot.commit_headers,
            &prepared.snapshot.rows,
            &prepared.snapshot.checkpoint_roots,
        )
        .await;
    match install {
        Ok(InitialSyncSnapshotInstall::Installed) => {
            lix.align_repository_identity_for_sync(prepared.lix_id)?;
            lix.align_primary_account_for_sync(prepared.transport.active_account_id())
                .await?;
            Ok(prepared.transport)
        }
        Ok(InitialSyncSnapshotInstall::ExistingRepository) => {
            let adapter = lix.storage_adapter();
            let _ = inspect_sync_bootstrap_with_adapter(
                &adapter,
                &server.url,
            )
            .await?;
            Err(restart_open_error())
        }
        Ok(InitialSyncSnapshotInstall::Ambiguous) => Err(ambiguous_replica_error()),
        Err(error) => Err(
            reconcile_install_error(
                &lix.storage_adapter(),
                &server.url,
                error,
            )
            .await,
        ),
    }
}

async fn reconcile_install_error<StorageImpl>(
    adapter: &StorageAdapter<StorageImpl>,
    remote_id: &str,
    error: LixError,
) -> LixError
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    if !is_ambiguous_bootstrap_write(&error) {
        return error;
    }
    match inspect_once(adapter, remote_id).await {
        Ok(BootstrapInspection::Ready { .. } | BootstrapInspection::Publishing) => {
            restart_open_error()
        }
        Ok(BootstrapInspection::Ambiguous) => ambiguous_replica_error(),
        Ok(BootstrapInspection::Prepare) | Err(_) => error,
    }
}

fn is_ambiguous_bootstrap_write(error: &LixError) -> bool {
    error.code == LixError::CODE_TRANSACTION_CONFLICT
        || error.code == LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN
}

fn restart_open_error() -> LixError {
    LixError::new(
        LixError::CODE_STORAGE_READ_EXPIRED,
        "sync bootstrap completed in another opener",
    )
    .with_details(serde_json::json!({ "retryable": true }))
}

fn ambiguous_replica_error() -> LixError {
    LixError::new(
        LixError::CODE_INVALID_PARAM,
        "sync replica contains conflicting durable authority receipts",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use crate::storage_adapter::{
        Memory, MemoryRead, MemoryWrite, StorageAdapterRead, StorageError,
        StorageWriteOptions,
    };

    #[derive(Clone)]
    struct TieredStorage {
        visible: Memory,
        durable: Memory,
    }

    #[derive(Clone)]
    struct DurableMemory {
        inner: Memory,
    }

    impl Storage for DurableMemory {
        type Read<'a> = MemoryRead;
        type Write<'a> = MemoryWrite;

        async fn acquire_session(
            &self,
        ) -> Result<crate::storage::StorageSessionToken, StorageError> {
            Err(StorageError::Unsupported(
                crate::storage::Capability::StorageSessions,
            ))
        }

        async fn begin_read(
            &self,
            mut options: StorageReadOptions,
        ) -> Result<Self::Read<'_>, StorageError> {
            options.durability = StorageReadDurability::Visible;
            self.inner.begin_read(options).await
        }

        async fn begin_write(
            &self,
            mut options: StorageWriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            options.await_durable = false;
            self.inner.begin_write(options).await
        }
    }

    impl Storage for TieredStorage {
        type Read<'a> = MemoryRead;
        type Write<'a> = MemoryWrite;

        async fn acquire_session(
            &self,
        ) -> Result<crate::storage::StorageSessionToken, StorageError> {
            Err(StorageError::Unsupported(
                crate::storage::Capability::StorageSessions,
            ))
        }

        async fn begin_read(
            &self,
            options: StorageReadOptions,
        ) -> Result<Self::Read<'_>, StorageError> {
            match options.durability {
                StorageReadDurability::Visible => self.visible.begin_read(options).await,
                StorageReadDurability::Durable => {
                    self.durable
                        .begin_read(StorageReadOptions {
                            durability: StorageReadDurability::Visible,
                            ..options
                        })
                        .await
                }
            }
        }

        async fn begin_write(
            &self,
            options: StorageWriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            self.visible.begin_write(options).await
        }
    }

    fn tiered(visible: Memory, durable: Memory) -> TieredStorage {
        TieredStorage {
            visible,
            durable,
        }
    }

    async fn initialized_memory() -> Memory {
        let storage = Memory::new();
        Engine::initialize_with_main_branch_id(storage.clone(), None)
            .await
            .expect("test storage should initialize");
        storage
    }

    async fn store_replica_state(storage: &Memory, _remote_id: &str) {
        store_replica_state_at_key(storage, b"repository", canonical_replica_state()).await;
    }

    async fn store_legacy_replica_state(storage: &Memory, remote_id: &str) {
        store_replica_state_at_key(
            storage,
            remote_id.as_bytes(),
            canonical_replica_state(),
        )
        .await;
    }

    fn canonical_replica_state() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "activeAccountId": crate::ANONYMOUS_ACCOUNT_ID,
            "cursor": 7,
            "authoritativeBranches": {},
            "authorityKnownCommitIds": []
        }))
        .expect("replica state should encode")
    }

    async fn store_replica_state_at_key(storage: &Memory, key: &[u8], value: Vec<u8>) {
        let adapter = StorageAdapter::new(storage.clone());
        let mut writes = adapter.new_write_set();
        writes.put(
            super::super::SYNC_REPLICA_STATE_SPACE,
            crate::storage_adapter::StorageKey(bytes::Bytes::copy_from_slice(
                key,
            )),
            value,
        );
        adapter
            .commit_certified_replica_write_set(
                crate::sync::certified_replica_write_capability(),
                writes,
                StorageWriteOptions::default(),
            )
            .await
            .expect("replica state should commit");
    }

    async fn store_malformed_replica_state(storage: &Memory, _remote_id: &str) {
        store_replica_state_at_key(
            storage,
            b"repository",
            br#"{"activeAccountId":"anonymous"}"#.to_vec(),
        )
        .await;
    }

    async fn replica_state_rows(storage: &Memory) -> Vec<(bytes::Bytes, bytes::Bytes)> {
        let adapter = StorageAdapter::new(storage.clone());
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("replica-state read should open");
        let mut cursor = read
            .begin_scan(
                super::super::SYNC_REPLICA_STATE_SPACE,
                crate::storage_adapter::StoragePrefix {
                    bytes: bytes::Bytes::new(),
                }
                .to_range()
                .expect("replica-state range should build"),
                crate::storage_adapter::StorageBeginScanOptions {
                    projection: crate::storage_adapter::StorageCoreProjection::FullValue,
                    ..crate::storage_adapter::StorageBeginScanOptions::default()
                },
            )
            .await
            .expect("replica-state scan should begin");
        let mut rows = Vec::new();
        while let Some(entries) = cursor
            .next_chunk()
            .await
            .expect("replica-state scan should advance")
        {
            for entry in entries {
                let crate::storage_adapter::StorageProjectedValue::FullValue(value) = entry.value
                else {
                    panic!("replica-state scan omitted its value");
                };
                rows.push((entry.key.0, value));
            }
        }
        rows
    }

    #[tokio::test]
    async fn legacy_url_key_migrates_byte_for_byte_and_accepts_a_new_url() {
        let storage = DurableMemory {
            inner: Memory::new(),
        };
        Engine::initialize_with_main_branch_id(storage.clone(), None)
            .await
            .expect("test storage should initialize");
        let old_url =
            "https://old.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001";
        let raw = format!(
            r#"{{ "activeAccountId":"{}", "cursor":7, "authoritativeBranches":{{}}, "pendingResets":{{}}, "authorityKnownCommitIds":[] }}"#,
            crate::ANONYMOUS_ACCOUNT_ID
        )
        .into_bytes();
        store_replica_state_at_key(&storage.inner, old_url.as_bytes(), raw.clone()).await;

        assert_eq!(
            inspect_sync_bootstrap(
                &storage,
                "https://new.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001",
            )
            .await
            .expect("legacy replica should migrate"),
            SyncBootstrapAdmission::Ready {
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            }
        );
        assert_eq!(
            replica_state_rows(&storage.inner).await,
            vec![(bytes::Bytes::from_static(b"repository"), bytes::Bytes::from(raw))],
            "migration must preserve the durable receipt and remove its URL key",
        );
        assert!(matches!(
            inspect_sync_bootstrap(
                &storage,
                "https://third.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001",
            )
            .await,
            Ok(SyncBootstrapAdmission::Ready { .. })
        ));
    }

    #[tokio::test]
    async fn malformed_durable_replica_state_fails_closed() {
        let visible = initialized_memory().await;
        let durable = initialized_memory().await;
        store_malformed_replica_state(&visible, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001").await;
        store_malformed_replica_state(&durable, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001").await;
        let storage = tiered(visible, durable);

        let error = inspect_sync_bootstrap(&storage, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001")
            .await
            .expect_err("malformed durable state must not trigger a fresh bootstrap");
        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
        assert!(error.message.contains("decode sync replica state"));
        assert!(error.message.contains("missing field `cursor`"));
    }

    #[tokio::test]
    async fn visible_replica_state_is_publishing_not_ready() {
        let visible = initialized_memory().await;
        let durable = initialized_memory().await;
        store_replica_state(&visible, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001").await;
        let storage = tiered(visible, durable);
        let adapter = StorageAdapter::new(storage);

        assert_eq!(
            inspect_once(&adapter, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001")
                .await
                .expect("bootstrap state should inspect"),
            BootstrapInspection::Publishing,
        );
    }

    #[tokio::test]
    async fn durable_exact_remote_is_the_ready_witness() {
        let visible = initialized_memory().await;
        let durable = initialized_memory().await;
        store_replica_state(&visible, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001").await;
        store_replica_state(&durable, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001").await;
        let storage = tiered(visible, durable);
        let adapter = StorageAdapter::new(storage);

        assert_eq!(
            inspect_once(&adapter, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001")
                .await
                .expect("bootstrap state should inspect"),
            BootstrapInspection::Ready {
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            },
        );
    }

    #[tokio::test]
    async fn durable_multiple_remote_bindings_fail_closed() {
        let visible = initialized_memory().await;
        let durable = initialized_memory().await;
        for storage in [&visible, &durable] {
            store_legacy_replica_state(storage, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001").await;
            store_legacy_replica_state(storage, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000002").await;
        }
        let storage = tiered(visible, durable);
        let error = inspect_sync_bootstrap(&storage, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001")
            .await
            .expect_err("multiple durable remotes must fail closed");
        assert_eq!(error.code, "LIX_ERROR_SYNC_REPLICA_STATE_AMBIGUOUS");
        assert!(error.message.contains("multiple durable authority receipts"));
    }

    #[tokio::test]
    async fn visible_ambiguous_binding_prevents_ready_admission() {
        let visible = initialized_memory().await;
        let durable = initialized_memory().await;
        store_replica_state(&visible, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001").await;
        store_legacy_replica_state(&visible, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000002").await;
        store_replica_state(&durable, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001").await;
        let storage = tiered(visible, durable);
        let adapter = StorageAdapter::new(storage);

        assert_eq!(
            inspect_once(&adapter, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001")
                .await
                .expect("bootstrap state should inspect"),
            BootstrapInspection::Publishing,
        );
    }

    #[tokio::test]
    async fn publishing_restarts_the_complete_open_until_the_receipt_is_durable() {
        let visible = initialized_memory().await;
        let durable = initialized_memory().await;
        store_replica_state(&visible, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001").await;
        let storage = tiered(visible, durable.clone());

        let error = inspect_sync_bootstrap(&storage, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001")
            .await
            .expect_err("visible state must restart rather than admit an opener");
        assert_eq!(error.code, LixError::CODE_STORAGE_READ_EXPIRED);
        store_replica_state(&durable, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001").await;

        assert_eq!(
            inspect_sync_bootstrap(&storage, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001")
                .await
                .expect("durable publication should admit the retried open"),
            SyncBootstrapAdmission::Ready {
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            },
        );
    }

    #[tokio::test]
    async fn semantic_install_errors_are_never_rewritten_as_a_race() {
        let visible = initialized_memory().await;
        let durable = initialized_memory().await;
        store_replica_state(&visible, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001").await;
        store_replica_state(&durable, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001").await;
        let storage = tiered(visible, durable);
        let original = LixError::new(LixError::CODE_INVALID_PARAM, "malformed snapshot");

        let error = reconcile_install_error(
            &StorageAdapter::new(storage.clone()),
            "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001",
            original.clone(),
        )
        .await;
        assert_eq!(error, original);
    }

    #[tokio::test]
    async fn ambiguous_install_write_reconciles_only_from_durable_binding_state() {
        let visible = initialized_memory().await;
        let durable = initialized_memory().await;
        store_replica_state(&visible, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001").await;
        store_replica_state(&durable, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001").await;
        let storage = tiered(visible, durable);

        let error = reconcile_install_error(
            &StorageAdapter::new(storage.clone()),
            "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001",
            LixError::new(LixError::CODE_TRANSACTION_CONFLICT, "lost publication race"),
        )
        .await;
        assert_eq!(error.code, LixError::CODE_STORAGE_READ_EXPIRED);
        assert_eq!(
            error.details,
            Some(serde_json::json!({ "retryable": true }))
        );
    }

    #[tokio::test]
    async fn ambiguous_install_write_restarts_while_the_winner_is_only_visible() {
        let visible = initialized_memory().await;
        let durable = initialized_memory().await;
        store_replica_state(&visible, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001").await;
        let storage = tiered(visible, durable);

        let error = reconcile_install_error(
            &StorageAdapter::new(storage.clone()),
            "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001",
            LixError::new(LixError::CODE_TRANSACTION_CONFLICT, "lost publication race"),
        )
        .await;
        assert_eq!(error.code, LixError::CODE_STORAGE_READ_EXPIRED);
        assert_eq!(
            error.details,
            Some(serde_json::json!({ "retryable": true }))
        );
    }

    #[tokio::test]
    async fn ambiguous_install_write_restarts_for_the_same_repository() {
        let visible = initialized_memory().await;
        let durable = initialized_memory().await;
        store_replica_state(&visible, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000002").await;
        store_replica_state(&durable, "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000002").await;
        let storage = tiered(visible, durable);

        let error = reconcile_install_error(
            &StorageAdapter::new(storage.clone()),
            "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-000000000001",
            LixError::new(LixError::CODE_TRANSACTION_CONFLICT, "lost publication race"),
        )
        .await;
        assert_eq!(error.code, LixError::CODE_STORAGE_READ_EXPIRED);
        assert_eq!(
            error.details,
            Some(serde_json::json!({ "retryable": true }))
        );
    }
}
