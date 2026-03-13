use crate::deterministic_mode::{
    build_persist_sequence_highest_batch, load_runtime_state, persist_sequence_highest,
    DeterministicRuntimeState, DeterministicSettings, RuntimeFunctionProvider,
};
use crate::engine::Engine;
use crate::functions::SharedFunctionProvider;
use crate::sql::execution::write_program_runner::execute_write_program_with_transaction;
use crate::state::internal::write_program::WriteProgram;
use crate::{LixBackend, LixError, LixTransaction};

impl Engine {
    pub(crate) async fn prepare_runtime_functions_with_backend(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<
        (
            DeterministicSettings,
            i64,
            SharedFunctionProvider<RuntimeFunctionProvider>,
        ),
        LixError,
    > {
        let runtime_state = if self.deterministic_boot_pending() {
            let persisted = load_runtime_state(backend).await?;
            DeterministicRuntimeState {
                settings: self
                    .boot_deterministic_settings()
                    .unwrap_or(persisted.settings),
                ..persisted
            }
        } else {
            load_runtime_state(backend).await?
        };
        let settings = runtime_state.settings;
        let sequence_start = if settings.enabled {
            runtime_state.next_sequence
        } else {
            0
        };
        let functions =
            SharedFunctionProvider::new(RuntimeFunctionProvider::new(settings, sequence_start));
        Ok((settings, sequence_start, functions))
    }

    pub(crate) async fn persist_runtime_sequence_with_backend(
        &self,
        backend: &dyn LixBackend,
        settings: DeterministicSettings,
        sequence_start: i64,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError> {
        if settings.enabled {
            let sequence_end = functions.with_lock(|provider| provider.next_sequence());
            if sequence_end > sequence_start {
                persist_sequence_highest(backend, sequence_end - 1).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn persist_runtime_sequence_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        settings: DeterministicSettings,
        sequence_start: i64,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError> {
        if settings.enabled {
            let sequence_end = functions.with_lock(|provider| provider.next_sequence());
            if sequence_end > sequence_start {
                let mut program = WriteProgram::new();
                program.push_batch(build_persist_sequence_highest_batch(
                    sequence_end - 1,
                    transaction.dialect(),
                )?);
                execute_write_program_with_transaction(transaction, program).await?;
            }
        }
        Ok(())
    }
}
