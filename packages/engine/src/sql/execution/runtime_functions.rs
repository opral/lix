use crate::deterministic_mode::{
    load_persisted_sequence_next, load_settings, persist_sequence_highest, DeterministicSettings,
    RuntimeFunctionProvider,
};
use crate::functions::SharedFunctionProvider;
use crate::{Engine, LixBackend, LixError};
use std::sync::atomic::Ordering;

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
        let mut settings = load_settings(backend).await?;
        if self.deterministic_boot_pending.load(Ordering::SeqCst) {
            if let Some(boot_settings) = self.boot_deterministic_settings {
                settings = boot_settings;
            }
        }
        let sequence_start = if settings.enabled {
            load_persisted_sequence_next(backend).await?
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
}
