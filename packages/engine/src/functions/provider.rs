use std::sync::{Arc, Mutex};

use crate::cel::CelFunctionProvider;

/// Engine-owned runtime function provider trait.
pub(crate) trait FunctionProvider: Send {
    fn uuid_v7(&mut self) -> uuid::Uuid;
    fn timestamp(&mut self) -> String;

    fn deterministic_sequence_persist_highest_seen(&self) -> Option<i64> {
        None
    }
}

pub(crate) type FunctionProviderHandle = SharedFunctionProvider<Box<dyn FunctionProvider + Send>>;

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

    pub(crate) fn call_timestamp(&self) -> String {
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
        SharedFunctionProvider::call_timestamp(self)
    }
}

impl<P> FunctionProvider for SharedFunctionProvider<P>
where
    P: FunctionProvider,
{
    fn uuid_v7(&mut self) -> uuid::Uuid {
        self.call_uuid_v7()
    }

    fn timestamp(&mut self) -> String {
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

    fn timestamp(&mut self) -> String {
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
        uuid::Uuid::now_v7()
    }

    fn timestamp(&mut self) -> String {
        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    }
}
