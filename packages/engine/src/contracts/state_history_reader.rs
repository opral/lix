use async_trait::async_trait;

use crate::common::LixError;

use super::{StateHistoryRequest, StateHistoryRow};

#[async_trait(?Send)]
pub trait CommittedStateHistoryReader {
    async fn load_committed_state_history_rows(
        &self,
        request: &StateHistoryRequest,
    ) -> Result<Vec<StateHistoryRow>, LixError>;
}
