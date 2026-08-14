use crate::LixError;
use crate::changelog::ChangeId;
use crate::common::LixTimestamp;
use crate::functions::{
    DeterministicFunctionProvider, DeterministicSequence, FunctionProvider, FunctionProviderHandle,
    SystemFunctionProvider, state,
};
use crate::hot_state::GlobalKeyValueRowCache;
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition, StorageWriteSet};

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
    ///
    /// `cache` memoizes the deterministic-state rows against the global
    /// branch-head control they were resolved under. Pass `None` from a caller
    /// that has no engine-lifetime cache to hand; the read is then always
    /// canonical.
    #[expect(trivial_casts)]
    pub(crate) async fn prepare(
        read: &(impl StorageAdapterRead + ?Sized),
        cache: Option<&GlobalKeyValueRowCache>,
    ) -> Result<Self, LixError> {
        let mode = state::load_mode(read, cache).await?;
        if !mode.enabled {
            return Ok(Self::system_for_function_free_read());
        }

        let sequence = state::load_sequence(read, cache).await?;
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

    /// Persists deterministic sequence progress if this execution used any.
    ///
    /// System functions report no sequence state, so this is a no-op when
    /// deterministic mode is disabled.
    pub(crate) async fn stage_persist_if_needed(
        &self,
        read: &(impl StorageAdapterRead + ?Sized),
        writes: &mut StorageWriteSet,
    ) -> Result<Vec<StoragePrecondition>, LixError> {
        let Some(highest_seen) = self.functions.deterministic_sequence_persist_highest_seen()
        else {
            return Ok(Vec::new());
        };
        state::stage_sequence(
            read,
            writes,
            DeterministicSequence { highest_seen },
            self.bookkeeping_timestamp,
            deterministic_sequence_change_id(highest_seen),
        )
        .await
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
    use crate::GLOBAL_BRANCH_ID;
    use crate::branch::{BranchHeadControlContext, stage_branch_head_control};
    use crate::entity_pk::EntityPk;
    use crate::functions::state::{DETERMINISTIC_MODE_KEY, DETERMINISTIC_SEQUENCE_KEY};
    use crate::functions::{DeterministicSequence, state::load_sequence};
    use crate::hot_state::HotStateContext;
    use crate::hot_state::{CurrentStateDeltaRef, TrackedHeadContext};
    use crate::storage_adapter::StorageAdapter;
    use crate::storage_adapter::{Memory, StorageReadOptions, StorageWriteOptions};

    use super::*;

    fn hot_state_context() -> HotStateContext {
        HotStateContext::new(
            crate::tracked_state::TrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        )
    }

    #[tokio::test]
    async fn prepare_uses_system_functions_when_mode_missing() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        let context = FunctionContext::prepare(&read, None)
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
        let storage = StorageAdapter::new(Memory::new());
        crate::test_support::seed_global_branch_head(storage.clone()).await;
        write_key_value(
            storage.clone(),
            DETERMINISTIC_MODE_KEY,
            serde_json::json!({
                "enabled": true,
            }),
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let context = FunctionContext::prepare(&read, None)
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
        let storage = StorageAdapter::new(Memory::new());
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

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let context = FunctionContext::prepare(&read, None)
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
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
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
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            FunctionContext::prepare(&read, None)
                .await
                .expect("runtime context should prepare")
        };
        context.provider().call_uuid_v7();

        let mut writes = storage.new_write_set();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        context
            .stage_persist_if_needed(&read, &mut writes)
            .await
            .expect("sequence should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("sequence should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let sequence = load_sequence(&read, None).await.expect("sequence should load");
        assert_eq!(sequence, DeterministicSequence { highest_seen: 0 });

        // Deterministic mode must stamp the bookkeeping row from the
        // persisted sequence, never from the system clock; the persisted
        // sequence was empty, so next_sequence is 0 -> epoch.
        let row = hot_state
            .reader(&read)
            .load_row(&crate::hot_state::HotStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                entity_pk: EntityPk::single(DETERMINISTIC_SEQUENCE_KEY),
                file_id: crate::NullableKeyFilter::Null,
            })
            .await
            .expect("sequence row should load")
            .expect("sequence row should exist");
        assert_eq!(
            row.created_at.to_string(),
            "1970-01-01T00:00:00.000Z",
            "bookkeeping timestamp must derive from the sequence, not the system clock"
        );
        assert_eq!(row.created_at, row.updated_at);
    }

    #[tokio::test]
    async fn persist_if_needed_is_noop_for_system_functions() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let context = FunctionContext::prepare(&read, None)
            .await
            .expect("runtime context should prepare");

        let mut writes = storage.new_write_set();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        context
            .stage_persist_if_needed(&read, &mut writes)
            .await
            .expect("persist should no-op");
        assert!(writes.is_empty());

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let sequence = load_sequence(&read, None)
            .await
            .expect("missing sequence should load");
        assert_eq!(sequence, DeterministicSequence::uninitialized());
    }

    async fn write_key_value(storage: StorageAdapter, key: &str, value: serde_json::Value) {
        let snapshot_value = serde_json::json!({
            "key": key,
            "value": value,
        });
        let snapshot_content = serde_json::to_string(&snapshot_value)
        .expect("snapshot should serialize");
        let timestamp = LixTimestamp::expect_parse("created_at", "1970-01-01T00:00:00.000Z");
        let entity_pk = EntityPk::single(key);
        let native_snapshot = crate::native_row::encode(
            crate::native_row::seed_schema("lix_key_value")
                .expect("lix_key_value schema should exist"),
            &entity_pk,
            GLOBAL_BRANCH_ID,
            None,
            true,
            &snapshot_value,
        )
        .expect("test key-value tuple should encode");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let control = BranchHeadControlContext::new()
            .reader(&read)
            .load(GLOBAL_BRANCH_ID)
            .await
            .expect("global branch control should load")
            .expect("global branch control should exist");
        let snapshot = crate::json_store::JsonSlot::from_json(&snapshot_content);
        let mut next_control = control
            .next_current_state_revision()
            .expect("global control revision should advance");
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_untracked_current_state(
                GLOBAL_BRANCH_ID,
                control.tracked_generation,
                &[CurrentStateDeltaRef {
                    schema_key: "lix_key_value",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: Some(ChangeId::for_test_label("functions-context-sequence")),
                    commit_id: None,
                    untracked: true,
                    deleted: false,
                    created_at: timestamp,
                    updated_at: timestamp,
                    snapshot: snapshot.as_ref_slot(),
                    native_snapshot: Some(&native_snapshot),
                    metadata: crate::json_store::JsonSlotRef::None,
                    columnar_base_coordinate: None,
                }],
                &std::collections::BTreeSet::new(),
            )
            .await
            .expect("test key-value current row should stage");
        next_control.note_schema("lix_key_value");
        stage_branch_head_control(&mut writes, GLOBAL_BRANCH_ID, next_control)
            .expect("global control should publish current state");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("test key-value should commit");
    }
}
