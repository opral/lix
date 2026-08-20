//! Platform-neutral transport contract consumed by the shared sync engine.
//!
//! The repository URL already selects one Lix repository. Accordingly the
//! live protocol has one cursor and no branch, schema, projection, or session
//! topology side channels. HTTP, task, timer, and cancellation mechanics live
//! under [`super::platform`].

use super::{
    SyncBlobManifest, SyncBlobRegistration, SyncHistoryResponse, SyncPushRequest, SyncPushResponse,
    SyncRepositoryPullResponse, SyncSnapshotRowPage, SyncTransportBounds, SyncTransportFuture,
};

/// Operations required from a remote repository authority.
///
/// Commits and ref updates use one ordered repository cursor. Binary payloads
/// use their independent BLAKE3/FastCDC CAS and are transferred only when a
/// commit references content absent on the receiving side.
pub trait SyncTransport: SyncTransportBounds {
    /// Account authenticated by the authority handshake for this session.
    fn active_account_id(&self) -> &str;

    /// Atomically publishes immutable commits and compare-and-swap ref moves.
    fn push<'a>(
        &'a self,
        request: &'a SyncPushRequest,
    ) -> SyncTransportFuture<'a, SyncPushResponse>;

    /// Loads hot state when `after` is `None`, otherwise long-polls the single
    /// ordered repository event stream after that cursor.
    fn pull(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> SyncTransportFuture<'_, SyncRepositoryPullResponse>;

    /// Loads one bounded hot-row page at an immutable branch head.
    fn snapshot_rows<'a>(
        &'a self,
        branch_id: &'a str,
        head_commit_id: &'a str,
        continuation: Option<&'a str>,
        limit: usize,
    ) -> SyncTransportFuture<'a, SyncSnapshotRowPage>;

    /// Loads one bounded first-parent history page without changing live sync
    /// state. A later missing ancestor becomes the head of the next demand.
    fn history<'a>(
        &'a self,
        head: &'a str,
        limit: usize,
    ) -> SyncTransportFuture<'a, SyncHistoryResponse>;

    /// Loads one canonical flat blob manifest, if it is registered.
    fn get_blob<'a>(
        &'a self,
        blob_id: &'a str,
    ) -> SyncTransportFuture<'a, Option<SyncBlobManifest>>;

    /// Negotiates missing chunks and atomically registers a verified manifest.
    fn register_blob<'a>(
        &'a self,
        manifest: &'a SyncBlobManifest,
    ) -> SyncTransportFuture<'a, SyncBlobRegistration>;

    /// Loads one raw BLAKE3-addressed chunk, if present.
    fn get_chunk<'a>(&'a self, chunk_id: &'a str) -> SyncTransportFuture<'a, Option<Vec<u8>>>;

    /// Stores one raw chunk after the authority verifies its BLAKE3 identity.
    fn put_chunk<'a>(&'a self, chunk_id: &'a str, bytes: &'a [u8]) -> SyncTransportFuture<'a, ()>;
}
