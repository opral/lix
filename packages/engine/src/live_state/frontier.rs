use crate::live_state::store::LiveStateFrontierReadStore;
use crate::version::CommittedVersionFrontier;
use crate::LixError;

pub(crate) async fn load_version_head_commit_id(
    store: &mut impl LiveStateFrontierReadStore,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    store.load_version_head_commit_id(version_id).await
}

pub(crate) async fn load_version_head_commit_map(
    store: &mut impl LiveStateFrontierReadStore,
) -> Result<Option<std::collections::BTreeMap<String, String>>, LixError> {
    store.load_version_head_commit_map().await
}

pub(crate) async fn load_current_committed_version_frontier(
    store: &mut impl LiveStateFrontierReadStore,
) -> Result<CommittedVersionFrontier, LixError> {
    store.load_current_committed_version_frontier().await
}
