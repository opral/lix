pub(crate) use super::history::{
    StateHistoryContentMode, StateHistoryLineageScope, StateHistoryRequest, StateHistoryRootScope,
    StateHistoryRow, StateHistoryVersionScope,
};
pub(crate) use crate::change_view::TrackedDomainChangeView;
pub(crate) use crate::filesystem::history::{
    DirectoryHistoryRequest, DirectoryHistoryRow, FileHistoryContentMode, FileHistoryLineageScope,
    FileHistoryRequest, FileHistoryRootScope, FileHistoryRow, FileHistoryVersionScope,
};
use crate::{LixBackend, LixError};

pub(crate) async fn load_state_history_rows(
    backend: &dyn LixBackend,
    request: &StateHistoryRequest,
) -> Result<Vec<StateHistoryRow>, LixError> {
    super::history::load_state_history_rows(backend, request).await
}

pub(crate) async fn load_file_history_rows(
    backend: &dyn LixBackend,
    request: &FileHistoryRequest,
) -> Result<Vec<FileHistoryRow>, LixError> {
    crate::filesystem::history::load_file_history_rows(backend, request).await
}

pub(crate) async fn load_directory_history_rows(
    backend: &dyn LixBackend,
    request: &DirectoryHistoryRequest,
) -> Result<Vec<DirectoryHistoryRow>, LixError> {
    crate::filesystem::history::load_directory_history_rows(backend, request).await
}
