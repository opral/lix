use async_trait::async_trait;

use crate::common::LixError;
use crate::{history::StateHistoryRow, LixBackend};

use super::{state_loader, StateHistoryRequest};

#[async_trait(?Send)]
pub trait CommittedStateHistoryReader {
    async fn load_committed_state_history_rows(
        &self,
        request: &StateHistoryRequest,
    ) -> Result<Vec<StateHistoryRow>, LixError>;
}

#[async_trait(?Send)]
impl CommittedStateHistoryReader for dyn LixBackend + '_ {
    async fn load_committed_state_history_rows(
        &self,
        request: &StateHistoryRequest,
    ) -> Result<Vec<StateHistoryRow>, LixError> {
        state_loader::load_state_history_rows(self, request).await
    }
}
