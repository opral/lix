//! Platform-neutral transport contract consumed by the shared sync engine.
//!
//! HTTP, task, timer, and cancellation mechanics live under [`super::platform`].
//! Admission, pull ordering, reconciliation, retry, and persistence policy do
//! not belong in a transport implementation.

use super::{
    SyncAdmission, SyncBranch, SyncPullResponse, SyncTransactionPack, SyncTransportBounds,
    SyncTransportFuture,
};

/// The operations required from a remote sync authority.
///
/// The Server Protocol is one transport for this interface. Keeping the core
/// state machine transport-neutral also lets embedded hosts and tests connect
/// two Lix repositories without making the database depend on an HTTP client.
pub trait SyncTransport: SyncTransportBounds {
    /// Stable identity of the remote repository (normally its canonical URL).
    fn remote_id(&self) -> &str;

    fn admit<'a>(&'a self, pack: &'a SyncTransactionPack)
    -> SyncTransportFuture<'a, SyncAdmission>;

    fn pull<'a>(
        &'a self,
        branch_id: &'a str,
        after_cursor: u64,
        limit: usize,
        // Empty means an unscoped/head-only request; non-empty requests only
        // materialize row/file payloads for these schema keys.
        schema_keys: &'a [String],
    ) -> SyncTransportFuture<'a, SyncPullResponse>;

    /// Returns the current global branch catalog. Transports that do not
    /// expose control-plane enumeration can leave this empty; row sync remains
    /// fully functional and the runtime retries topology after reconnect.
    fn list_branches<'a>(&'a self) -> SyncTransportFuture<'a, Vec<SyncBranch>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    /// Whether [`list_branches`](Self::list_branches) is an authoritative
    /// catalog for this transport. The default is deliberately false: an
    /// empty result from an embedded or test transport must not be interpreted
    /// as proof that every local branch was deleted. Only an explicit catalog
    /// implementation may drive destructive branch pruning.
    fn has_authoritative_branch_catalog(&self) -> bool {
        false
    }
}
