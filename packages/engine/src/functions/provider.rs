use std::sync::{Arc, Mutex};

use crate::cel::CelFunctionProvider;
use crate::common::LixTimestamp;

/// Engine-owned runtime function provider trait.
pub(crate) trait FunctionProvider: Send {
    fn uuid_v7(&mut self) -> uuid::Uuid;
    fn timestamp(&mut self) -> LixTimestamp;

    fn deterministic_sequence_persist_highest_seen(&self) -> Option<i64> {
        None
    }
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
}

impl CelFunctionProvider for FunctionProviderHandle {
    fn call_uuid_v7(&self) -> uuid::Uuid {
        FunctionProviderHandle::call_uuid_v7(self)
    }

    fn call_timestamp(&self) -> String {
        FunctionProviderHandle::call_timestamp(self).to_string()
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
        self.with_lock_mut(|provider| provider.uuid_v7())
    }

    pub(crate) fn call_timestamp(&self) -> LixTimestamp {
        self.with_lock_mut(|provider| provider.timestamp())
    }

    pub(crate) fn deterministic_sequence_persist_highest_seen(&self) -> Option<i64> {
        self.with_lock(|provider| provider.deterministic_sequence_persist_highest_seen())
    }
}

impl<P> CelFunctionProvider for SharedFunctionProvider<P>
where
    P: FunctionProvider + Send + 'static,
{
    fn call_uuid_v7(&self) -> uuid::Uuid {
        SharedFunctionProvider::call_uuid_v7(self)
    }

    fn call_timestamp(&self) -> String {
        SharedFunctionProvider::call_timestamp(self).to_string()
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
        SharedFunctionProvider::deterministic_sequence_persist_highest_seen(self)
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
