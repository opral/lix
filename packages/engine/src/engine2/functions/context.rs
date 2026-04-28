use crate::engine2::functions::{
    state, DeterministicFunctionProvider, DeterministicSequence, FunctionProvider,
    FunctionProviderHandle, SharedFunctionProvider, SystemFunctionProvider,
};
use crate::engine2::live_state::{LiveStateContextWriter, LiveStateReader};
use crate::LixError;

/// Execution-scoped runtime function context.
///
/// Lower layers should only receive function providers. This context owns the
/// lifecycle at the session/transaction boundary: prepare the right function
/// source before execution and persist deterministic sequence progress after
/// successful execution.
pub(crate) struct FunctionContext {
    functions: FunctionProviderHandle,
    bookkeeping_timestamp: String,
}

impl FunctionContext {
    /// Prepares the runtime function provider for one execution.
    ///
    /// If deterministic mode is absent or disabled, the context uses system
    /// functions. If enabled, it starts from the persisted sequence + 1.
    pub(crate) async fn prepare(live_state: &dyn LiveStateReader) -> Result<Self, LixError> {
        let mode = state::load_mode(live_state).await?;
        let mut bookkeeping_functions = SystemFunctionProvider;
        let bookkeeping_timestamp = bookkeeping_functions.timestamp();
        if !mode.enabled {
            return Ok(Self {
                functions: SharedFunctionProvider::new(
                    Box::new(SystemFunctionProvider) as Box<dyn FunctionProvider + Send>
                ),
                bookkeeping_timestamp,
            });
        }

        let sequence = state::load_sequence(live_state).await?;
        Ok(Self {
            functions: SharedFunctionProvider::new(Box::new(DeterministicFunctionProvider::new(
                sequence.next_sequence(),
                mode.timestamp_shuffle,
            ))
                as Box<dyn FunctionProvider + Send>),
            bookkeeping_timestamp,
        })
    }

    /// Returns the engine2-owned provider used by SQL and transaction staging.
    pub(crate) fn provider(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    /// Persists deterministic sequence progress if this execution used any.
    ///
    /// System functions report no sequence state, so this is a no-op when
    /// deterministic mode is disabled.
    pub(crate) async fn persist_if_needed<S>(
        &self,
        writer: &mut LiveStateContextWriter<S>,
    ) -> Result<(), LixError>
    where
        S: crate::backend::KvWriter,
    {
        let Some(highest_seen) = self.functions.deterministic_sequence_persist_highest_seen()
        else {
            return Ok(());
        };
        state::write_sequence(
            writer,
            DeterministicSequence { highest_seen },
            &self.bookkeeping_timestamp,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
    use crate::engine2::functions::state::{DETERMINISTIC_MODE_KEY, DETERMINISTIC_SEQUENCE_KEY};
    use crate::engine2::functions::{state::load_sequence, DeterministicSequence};
    use crate::engine2::live_state::{LiveStateContext, LiveStateRow};
    use crate::version::GLOBAL_VERSION_ID;

    use super::*;

    #[tokio::test]
    async fn prepare_uses_system_functions_when_mode_missing() {
        let backend = Arc::new(UnitTestBackend::new());
        let live_state = LiveStateContext::new(
            crate::engine2::tracked_state::TrackedStateContext::new(),
            crate::engine2::untracked_state::UntrackedStateContext::new(),
        );
        let reader = live_state.reader(Arc::clone(&backend));

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
        let backend = Arc::new(UnitTestBackend::new());
        let live_state = LiveStateContext::new(
            crate::engine2::tracked_state::TrackedStateContext::new(),
            crate::engine2::untracked_state::UntrackedStateContext::new(),
        );
        write_key_value(
            Arc::clone(&backend),
            &live_state,
            DETERMINISTIC_MODE_KEY,
            serde_json::json!({
                "enabled": true,
            }),
        )
        .await;

        let reader = live_state.reader(Arc::clone(&backend));
        let context = FunctionContext::prepare(&reader)
            .await
            .expect("runtime context should prepare");
        let functions = context.provider();

        assert_eq!(
            functions.call_uuid_v7(),
            "01920000-0000-7000-8000-000000000000"
        );
        assert_eq!(functions.call_timestamp(), "1970-01-01T00:00:00.001Z");
        assert_eq!(
            context
                .provider()
                .deterministic_sequence_persist_highest_seen(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn prepare_continues_from_persisted_sequence() {
        let backend = Arc::new(UnitTestBackend::new());
        let live_state = LiveStateContext::new(
            crate::engine2::tracked_state::TrackedStateContext::new(),
            crate::engine2::untracked_state::UntrackedStateContext::new(),
        );
        write_key_value(
            Arc::clone(&backend),
            &live_state,
            DETERMINISTIC_MODE_KEY,
            serde_json::json!({
                "enabled": true,
            }),
        )
        .await;
        write_key_value(
            Arc::clone(&backend),
            &live_state,
            DETERMINISTIC_SEQUENCE_KEY,
            serde_json::json!(41),
        )
        .await;

        let reader = live_state.reader(Arc::clone(&backend));
        let context = FunctionContext::prepare(&reader)
            .await
            .expect("runtime context should prepare");
        let functions = context.provider();

        assert_eq!(
            functions.call_uuid_v7(),
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
        let backend = Arc::new(UnitTestBackend::new());
        let live_state = LiveStateContext::new(
            crate::engine2::tracked_state::TrackedStateContext::new(),
            crate::engine2::untracked_state::UntrackedStateContext::new(),
        );
        write_key_value(
            Arc::clone(&backend),
            &live_state,
            DETERMINISTIC_MODE_KEY,
            serde_json::json!({
                "enabled": true,
            }),
        )
        .await;

        let context = {
            let reader = live_state.reader(Arc::clone(&backend));
            FunctionContext::prepare(&reader)
                .await
                .expect("runtime context should prepare")
        };
        context.provider().call_uuid_v7();

        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        context
            .persist_if_needed(&mut live_state.writer(tx.as_mut()))
            .await
            .expect("sequence should persist");
        tx.commit().await.expect("transaction should commit");

        let reader = live_state.reader(Arc::clone(&backend));
        let sequence = load_sequence(&reader).await.expect("sequence should load");
        assert_eq!(sequence, DeterministicSequence { highest_seen: 0 });
    }

    #[tokio::test]
    async fn persist_if_needed_is_noop_for_system_functions() {
        let backend = Arc::new(UnitTestBackend::new());
        let live_state = LiveStateContext::new(
            crate::engine2::tracked_state::TrackedStateContext::new(),
            crate::engine2::untracked_state::UntrackedStateContext::new(),
        );
        let reader = live_state.reader(Arc::clone(&backend));
        let context = FunctionContext::prepare(&reader)
            .await
            .expect("runtime context should prepare");

        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        context
            .persist_if_needed(&mut live_state.writer(tx.as_mut()))
            .await
            .expect("persist should no-op");
        tx.commit().await.expect("transaction should commit");

        let reader = live_state.reader(Arc::clone(&backend));
        let sequence = load_sequence(&reader)
            .await
            .expect("missing sequence should load");
        assert_eq!(sequence, DeterministicSequence::uninitialized());
    }

    async fn write_key_value(
        backend: Arc<UnitTestBackend>,
        live_state: &LiveStateContext,
        key: &str,
        value: serde_json::Value,
    ) {
        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let snapshot_content = serde_json::to_string(&serde_json::json!({
            "key": key,
            "value": value,
        }))
        .expect("snapshot should serialize");
        let row = LiveStateRow {
            entity_id: key.to_string(),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(snapshot_content),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "1970-01-01T00:00:00.000Z".to_string(),
            updated_at: "1970-01-01T00:00:00.000Z".to_string(),
            global: true,
            change_id: None,
            commit_id: None,
            untracked: true,
            version_id: GLOBAL_VERSION_ID.to_string(),
        };
        live_state
            .writer(tx.as_mut())
            .write_rows(&[row])
            .await
            .expect("test key-value should write");
        tx.commit().await.expect("transaction should commit");
    }
}
