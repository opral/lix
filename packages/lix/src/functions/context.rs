use crate::LixError;
use crate::changelog::ChangeId;
use crate::common::LixTimestamp;
use crate::functions::{
    DeterministicFunctionProvider, FunctionProvider, FunctionProviderHandle,
    SystemFunctionProvider, state,
};
use crate::state::ForkTreeStateView;
use crate::storage_adapter::StorageAdapterRead;

/// Execution-scoped runtime function context.
///
/// Lower layers should only receive function providers. This context owns the
/// lifecycle at the session/transaction boundary: prepare the right function
/// source before execution and persist deterministic sequence progress after
/// successful execution.
pub(crate) struct FunctionContext {
    functions: FunctionProviderHandle,
    bookkeeping_timestamp: LixTimestamp,
    deterministic_mode_enabled: bool,
}

impl FunctionContext {
    /// Creates the runtime bookkeeping context for a read that cannot invoke
    /// SQL functions. Such a read never advances deterministic state, so it
    /// does not need to load the persisted deterministic-mode rows first.
    pub(crate) fn system_for_function_free_read() -> Self {
        let mut bookkeeping_functions = SystemFunctionProvider;
        Self {
            functions: FunctionProviderHandle::system(),
            bookkeeping_timestamp: bookkeeping_functions.timestamp(),
            deterministic_mode_enabled: false,
        }
    }

    /// Prepares the runtime function provider for one execution.
    ///
    /// If deterministic mode is absent or disabled, the context uses system
    /// functions. If enabled, it starts from the persisted sequence + 1.
    pub(crate) async fn prepare(
        read: &(impl StorageAdapterRead + ?Sized),
    ) -> Result<Self, LixError> {
        let facade = crate::forktree::ForkTreeReadFacade::new(read);
        let global_state_view =
            ForkTreeStateView::from_facade(facade, crate::GLOBAL_BRANCH_ID).await?;
        Self::prepare_from_view(&global_state_view).await
    }

    #[expect(trivial_casts)]
    async fn prepare_from_view<R>(state_view: &ForkTreeStateView<R>) -> Result<Self, LixError>
    where
        R: StorageAdapterRead,
    {
        let mode = state::load_mode(state_view).await?;
        if !mode.enabled {
            return Ok(Self::system_for_function_free_read());
        }

        let sequence = state::load_sequence(state_view).await?;
        // Deterministic mode must produce byte-identical state across runs;
        // bookkeeping rows (sequence persistence) take a timestamp derived
        // from the persisted sequence instead of the system clock, without
        // consuming a sequence tick from user-visible functions. The value
        // is intentionally un-shuffled: timestamp_shuffle exists to break
        // ordering assumptions on user-visible timestamps only.
        let bookkeeping_timestamp =
            LixTimestamp::from_unix_millis_utc_lossy(sequence.next_sequence());
        Ok(Self {
            functions: FunctionProviderHandle::shared(Box::new(DeterministicFunctionProvider::new(
                sequence.next_sequence(),
                mode.timestamp_shuffle,
            ))
                as Box<dyn FunctionProvider + Send>),
            bookkeeping_timestamp,
            deterministic_mode_enabled: true,
        })
    }

    pub(crate) fn deterministic_mode_enabled(&self) -> bool {
        self.deterministic_mode_enabled
    }

    /// Returns the engine-owned provider used by SQL and transaction staging.
    pub(crate) fn provider(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    pub(crate) fn deterministic_sequence_checkpoint(
        &self,
    ) -> Option<(i64, LixTimestamp, ChangeId)> {
        let highest_seen = self
            .functions
            .deterministic_sequence_persist_highest_seen()?;
        Some((
            highest_seen,
            self.bookkeeping_timestamp,
            deterministic_sequence_change_id(highest_seen),
        ))
    }
}

fn deterministic_sequence_change_id(highest_seen: i64) -> ChangeId {
    let hash = blake3::hash(format!("lix-deterministic-sequence:{highest_seen}").as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hash.as_bytes()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    ChangeId::from(uuid::Uuid::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_sequence_change_identity_is_stable_for_one_value() {
        let first = deterministic_sequence_change_id(7);
        let second = deterministic_sequence_change_id(7);
        let different = deterministic_sequence_change_id(8);
        assert_eq!(first, second);
        assert_ne!(first, different);
    }
}
