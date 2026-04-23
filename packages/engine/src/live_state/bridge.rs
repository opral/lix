use async_trait::async_trait;

use crate::common::LixError;

use super::ReplayCursor;
use super::SchemaRegistration;

#[async_trait]
pub trait LiveStateTransactionBridge: Send {
    async fn register_live_state_schema(
        &mut self,
        registration: &SchemaRegistration,
    ) -> Result<(), LixError>;

    async fn advance_live_state_replay_boundary(
        &mut self,
        replay_cursor: &ReplayCursor,
    ) -> Result<(), LixError>;
}
