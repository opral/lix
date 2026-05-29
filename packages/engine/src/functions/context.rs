use crate::LixError;
use crate::common::LixTimestamp;
use crate::functions::{
    DeterministicFunctionProvider, DeterministicSequence, FunctionProvider, FunctionProviderHandle,
    SystemFunctionProvider, state,
};
use crate::live_state::LiveStateReader;
use crate::storage::StorageWriteSet;

/// Execution-scoped runtime function context.
///
/// Lower layers should only receive function providers. This context owns the
/// lifecycle at the session/transaction boundary: prepare the right function
/// source before execution and persist deterministic sequence progress after
/// successful execution.
pub(crate) struct FunctionContext {
    functions: FunctionProviderHandle,
    bookkeeping_timestamp: LixTimestamp,
}

impl FunctionContext {
    /// Prepares the runtime function provider for one execution.
    ///
    /// If deterministic mode is absent or disabled, the context uses system
    /// functions. If enabled, it starts from the persisted sequence + 1.
    #[expect(trivial_casts)]
    pub(crate) async fn prepare(live_state: &dyn LiveStateReader) -> Result<Self, LixError> {
        let mode = state::load_mode(live_state).await?;
        let mut bookkeeping_functions = SystemFunctionProvider;
        let bookkeeping_timestamp = bookkeeping_functions.timestamp();
        if !mode.enabled {
            return Ok(Self {
                functions: FunctionProviderHandle::system(),
                bookkeeping_timestamp,
            });
        }

        let sequence = state::load_sequence(live_state).await?;
        Ok(Self {
            functions: FunctionProviderHandle::shared(Box::new(DeterministicFunctionProvider::new(
                sequence.next_sequence(),
                mode.timestamp_shuffle,
            ))
                as Box<dyn FunctionProvider + Send>),
            bookkeeping_timestamp,
        })
    }

    /// Returns the engine-owned provider used by SQL and transaction staging.
    pub(crate) fn provider(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    /// Persists deterministic sequence progress if this execution used any.
    ///
    /// System functions report no sequence state, so this is a no-op when
    /// deterministic mode is disabled.
    pub(crate) async fn stage_persist_if_needed(
        &self,
        writes: &mut StorageWriteSet,
    ) -> Result<(), LixError> {
        let Some(highest_seen) = self.functions.deterministic_sequence_persist_highest_seen()
        else {
            return Ok(());
        };
        state::stage_sequence(
            writes,
            DeterministicSequence { highest_seen },
            self.bookkeeping_timestamp,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::GLOBAL_BRANCH_ID;
    use crate::functions::state::{DETERMINISTIC_MODE_KEY, DETERMINISTIC_SEQUENCE_KEY};
    use crate::functions::{DeterministicSequence, state::load_sequence};
    use crate::live_state::LiveStateContext;
    use crate::storage::StorageContext;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};

    use super::*;

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            crate::tracked_state::TrackedStateContext::new(),
            crate::untracked_state::UntrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        )
    }

    #[tokio::test]
    async fn prepare_uses_system_functions_when_mode_missing() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let live_state = live_state_context();
        let reader = live_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );

        let context = FunctionContext::prepare(&reader)
            .await
            .expect("runtime context should prepare");

        assert_eq!(
            context
                .provider()
                .deterministic_sequence_persist_highest_seen(),
            None
        );
    }

    #[tokio::test]
    async fn prepare_starts_deterministic_functions_at_sequence_zero() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let live_state = live_state_context();
        crate::test_support::seed_global_branch_head(storage.clone()).await;
        write_key_value(
            storage.clone(),
            DETERMINISTIC_MODE_KEY,
            serde_json::json!({
                "enabled": true,
            }),
        )
        .await;

        let reader = live_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let context = FunctionContext::prepare(&reader)
            .await
            .expect("runtime context should prepare");
        let functions = context.provider();

        assert_eq!(
            functions.call_uuid_v7().to_string(),
            "01920000-0000-7000-8000-000000000000"
        );
        assert_eq!(
            functions.call_timestamp().to_string(),
            "1970-01-01T00:00:00.001Z"
        );
        assert_eq!(
            context
                .provider()
                .deterministic_sequence_persist_highest_seen(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn prepare_continues_from_persisted_sequence() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let live_state = live_state_context();
        crate::test_support::seed_global_branch_head(storage.clone()).await;
        write_key_value(
            storage.clone(),
            DETERMINISTIC_MODE_KEY,
            serde_json::json!({
                "enabled": true,
            }),
        )
        .await;
        write_key_value(
            storage.clone(),
            DETERMINISTIC_SEQUENCE_KEY,
            serde_json::json!(41),
        )
        .await;

        let reader = live_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let context = FunctionContext::prepare(&reader)
            .await
            .expect("runtime context should prepare");
        let functions = context.provider();

        assert_eq!(
            functions.call_uuid_v7().to_string(),
            "01920000-0000-7000-8000-00000000002a"
        );
        assert_eq!(
            context
                .provider()
                .deterministic_sequence_persist_highest_seen(),
            Some(42)
        );
    }

    #[tokio::test]
    async fn persist_if_needed_writes_sequence_when_deterministic_functions_advanced() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let live_state = live_state_context();
        crate::test_support::seed_global_branch_head(storage.clone()).await;
        write_key_value(
            storage.clone(),
            DETERMINISTIC_MODE_KEY,
            serde_json::json!({
                "enabled": true,
            }),
        )
        .await;

        let context = {
            let reader = live_state.reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            );
            FunctionContext::prepare(&reader)
                .await
                .expect("runtime context should prepare")
        };
        context.provider().call_uuid_v7();

        let mut writes = storage.new_write_set();
        context
            .stage_persist_if_needed(&mut writes)
            .await
            .expect("sequence should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("sequence should commit");

        let reader = live_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let sequence = load_sequence(&reader).await.expect("sequence should load");
        assert_eq!(sequence, DeterministicSequence { highest_seen: 0 });
    }

    #[tokio::test]
    async fn persist_if_needed_is_noop_for_system_functions() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let live_state = live_state_context();
        let reader = live_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let context = FunctionContext::prepare(&reader)
            .await
            .expect("runtime context should prepare");

        let mut writes = storage.new_write_set();
        context
            .stage_persist_if_needed(&mut writes)
            .await
            .expect("persist should no-op");
        assert!(writes.is_empty());

        let reader = live_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let sequence = load_sequence(&reader)
            .await
            .expect("missing sequence should load");
        assert_eq!(sequence, DeterministicSequence::uninitialized());
    }

    async fn write_key_value(storage: StorageContext, key: &str, value: serde_json::Value) {
        let snapshot_content = serde_json::to_string(&serde_json::json!({
            "key": key,
            "value": value,
        }))
        .expect("snapshot should serialize");
        let mut writes = storage.new_write_set();
        let row = crate::untracked_state::UntrackedStateRow {
            entity_pk: crate::entity_pk::EntityPk::single(key),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some(snapshot_content),
            metadata: None,
            created_at: LixTimestamp::expect_parse("created_at", "1970-01-01T00:00:00.000Z"),
            updated_at: LixTimestamp::expect_parse("updated_at", "1970-01-01T00:00:00.000Z"),
            global: true,
            branch_id: GLOBAL_BRANCH_ID.to_string(),
        };
        crate::untracked_state::UntrackedStateContext::new()
            .writer(&mut writes)
            .stage_rows(std::iter::once(row.as_ref()))
            .expect("test key-value should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("test key-value should commit");
    }
}
