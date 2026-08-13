use std::sync::{Arc, Mutex};

use crate::common::LixTimestamp;

/// Restorable runtime-function state captured around an explicit SQL
/// statement. Only deterministic providers have durable state that must be
/// rewound when a post-stage statement error is rolled back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FunctionProviderCheckpoint {
    DeterministicSequence {
        next_sequence: i64,
        highest_seen: Option<i64>,
    },
}

/// Engine-owned runtime function provider trait.
pub(crate) trait FunctionProvider: Send {
    fn uuid_v7(&mut self) -> uuid::Uuid;
    fn timestamp(&mut self) -> LixTimestamp;

    fn deterministic_sequence_persist_highest_seen(&self) -> Option<i64> {
        None
    }

    fn statement_checkpoint(&self) -> Option<FunctionProviderCheckpoint> {
        None
    }

    fn restore_statement_checkpoint(&mut self, _checkpoint: FunctionProviderCheckpoint) {}
}

#[derive(Clone)]
pub(crate) enum FunctionProviderHandle {
    System,
    Shared(SharedFunctionProvider<Box<dyn FunctionProvider + Send>>),
}

impl FunctionProviderHandle {
    pub(crate) fn system() -> Self {
        Self::System
    }

    pub(crate) fn shared(provider: Box<dyn FunctionProvider + Send>) -> Self {
        Self::Shared(SharedFunctionProvider::new(provider))
    }

    pub(crate) fn call_uuid_v7(&self) -> uuid::Uuid {
        match self {
            Self::System => SystemFunctionProvider::uuid_v7_now(),
            Self::Shared(provider) => provider.call_uuid_v7(),
        }
    }

    pub(crate) fn call_timestamp(&self) -> LixTimestamp {
        match self {
            Self::System => SystemFunctionProvider::timestamp_now(),
            Self::Shared(provider) => provider.call_timestamp(),
        }
    }

    pub(crate) fn deterministic_sequence_persist_highest_seen(&self) -> Option<i64> {
        match self {
            Self::System => None,
            Self::Shared(provider) => provider.deterministic_sequence_persist_highest_seen(),
        }
    }

    pub(crate) fn statement_checkpoint(&self) -> Option<FunctionProviderCheckpoint> {
        match self {
            Self::System => None,
            Self::Shared(provider) => provider.statement_checkpoint(),
        }
    }

    pub(crate) fn restore_statement_checkpoint(&self, checkpoint: FunctionProviderCheckpoint) {
        if let Self::Shared(provider) = self {
            provider.restore_statement_checkpoint(checkpoint);
        }
    }
}

/// Shareable function provider used across SQL planning, UDFs, and staging.
pub(crate) struct SharedFunctionProvider<P> {
    inner: Arc<Mutex<P>>,
}

impl<P> Clone for SharedFunctionProvider<P> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<P> SharedFunctionProvider<P> {
    pub(crate) fn new(provider: P) -> Self {
        Self {
            inner: Arc::new(Mutex::new(provider)),
        }
    }

    fn with_lock<R>(&self, f: impl FnOnce(&P) -> R) -> R {
        let guard = self
            .inner
            .lock()
            .expect("engine function provider mutex poisoned");
        f(&guard)
    }

    fn with_lock_mut<R>(&self, f: impl FnOnce(&mut P) -> R) -> R {
        let mut guard = self
            .inner
            .lock()
            .expect("engine function provider mutex poisoned");
        f(&mut guard)
    }
}

impl<P> SharedFunctionProvider<P>
where
    P: FunctionProvider,
{
    pub(crate) fn call_uuid_v7(&self) -> uuid::Uuid {
        self.with_lock_mut(FunctionProvider::uuid_v7)
    }

    pub(crate) fn call_timestamp(&self) -> LixTimestamp {
        self.with_lock_mut(FunctionProvider::timestamp)
    }

    pub(crate) fn deterministic_sequence_persist_highest_seen(&self) -> Option<i64> {
        self.with_lock(FunctionProvider::deterministic_sequence_persist_highest_seen)
    }

    pub(crate) fn statement_checkpoint(&self) -> Option<FunctionProviderCheckpoint> {
        self.with_lock(FunctionProvider::statement_checkpoint)
    }

    pub(crate) fn restore_statement_checkpoint(&self, checkpoint: FunctionProviderCheckpoint) {
        self.with_lock_mut(|provider| provider.restore_statement_checkpoint(checkpoint));
    }
}

impl<P> FunctionProvider for SharedFunctionProvider<P>
where
    P: FunctionProvider,
{
    fn uuid_v7(&mut self) -> uuid::Uuid {
        self.call_uuid_v7()
    }

    fn timestamp(&mut self) -> LixTimestamp {
        self.call_timestamp()
    }

    fn deterministic_sequence_persist_highest_seen(&self) -> Option<i64> {
        Self::deterministic_sequence_persist_highest_seen(self)
    }

    fn statement_checkpoint(&self) -> Option<FunctionProviderCheckpoint> {
        Self::statement_checkpoint(self)
    }

    fn restore_statement_checkpoint(&mut self, checkpoint: FunctionProviderCheckpoint) {
        Self::restore_statement_checkpoint(self, checkpoint);
    }
}

impl<T> FunctionProvider for Box<T>
where
    T: FunctionProvider + ?Sized,
{
    fn uuid_v7(&mut self) -> uuid::Uuid {
        (**self).uuid_v7()
    }

    fn timestamp(&mut self) -> LixTimestamp {
        (**self).timestamp()
    }

    fn deterministic_sequence_persist_highest_seen(&self) -> Option<i64> {
        (**self).deterministic_sequence_persist_highest_seen()
    }

    fn statement_checkpoint(&self) -> Option<FunctionProviderCheckpoint> {
        (**self).statement_checkpoint()
    }

    fn restore_statement_checkpoint(&mut self, checkpoint: FunctionProviderCheckpoint) {
        (**self).restore_statement_checkpoint(checkpoint);
    }
}

/// System-backed engine function provider.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SystemFunctionProvider;

impl FunctionProvider for SystemFunctionProvider {
    fn uuid_v7(&mut self) -> uuid::Uuid {
        Self::uuid_v7_now()
    }

    fn timestamp(&mut self) -> LixTimestamp {
        Self::timestamp_now()
    }
}

impl SystemFunctionProvider {
    fn uuid_v7_now() -> uuid::Uuid {
        uuid::Uuid::now_v7()
    }

    fn timestamp_now() -> LixTimestamp {
        LixTimestamp::from_unix_millis_utc_lossy(chrono::Utc::now().timestamp_millis())
    }
}
