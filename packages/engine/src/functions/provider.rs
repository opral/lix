use std::sync::{Arc, Mutex};

pub trait LixFunctionProvider {
    fn uuid_v7(&mut self) -> String;
    fn timestamp(&mut self) -> String;

    fn deterministic_sequence_enabled(&self) -> bool {
        false
    }

    fn deterministic_sequence_initialized(&self) -> bool {
        true
    }

    fn initialize_deterministic_sequence(&mut self, _sequence_start: i64) {}

    fn deterministic_sequence_persist_highest_seen(&self) -> Option<i64> {
        None
    }
}

pub type DynFunctionProvider = SharedFunctionProvider<Box<dyn LixFunctionProvider + Send>>;

pub struct SharedFunctionProvider<P> {
    inner: Arc<Mutex<P>>,
}

struct SharedFunctionProviderForwarder<P> {
    provider: SharedFunctionProvider<P>,
}

impl<P> Clone for SharedFunctionProvider<P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<P> SharedFunctionProvider<P> {
    pub fn new(provider: P) -> Self {
        Self {
            inner: Arc::new(Mutex::new(provider)),
        }
    }

    pub fn with_lock<R>(&self, f: impl FnOnce(&P) -> R) -> R {
        let guard = self.inner.lock().expect("function provider mutex poisoned");
        f(&guard)
    }

    fn with_lock_mut<R>(&self, f: impl FnOnce(&mut P) -> R) -> R {
        let mut guard = self.inner.lock().expect("function provider mutex poisoned");
        f(&mut guard)
    }
}

impl<P: LixFunctionProvider> SharedFunctionProvider<P> {
    pub fn call_uuid_v7(&self) -> String {
        self.with_lock_mut(|provider| provider.uuid_v7())
    }

    pub fn call_timestamp(&self) -> String {
        self.with_lock_mut(|provider| provider.timestamp())
    }
}

pub fn clone_boxed_function_provider<P>(provider: &SharedFunctionProvider<P>) -> DynFunctionProvider
where
    P: LixFunctionProvider + Send + 'static,
{
    SharedFunctionProvider::new(Box::new(SharedFunctionProviderForwarder {
        provider: provider.clone(),
    }) as Box<dyn LixFunctionProvider + Send>)
}

impl<P> LixFunctionProvider for SharedFunctionProviderForwarder<P>
where
    P: LixFunctionProvider + Send + 'static,
{
    fn uuid_v7(&mut self) -> String {
        self.provider.call_uuid_v7()
    }

    fn timestamp(&mut self) -> String {
        self.provider.call_timestamp()
    }

    fn deterministic_sequence_enabled(&self) -> bool {
        self.provider.deterministic_sequence_enabled()
    }

    fn deterministic_sequence_initialized(&self) -> bool {
        self.provider.deterministic_sequence_initialized()
    }

    fn initialize_deterministic_sequence(&mut self, sequence_start: i64) {
        self.provider
            .initialize_deterministic_sequence(sequence_start);
    }

    fn deterministic_sequence_persist_highest_seen(&self) -> Option<i64> {
        self.provider.deterministic_sequence_persist_highest_seen()
    }
}

impl<P: LixFunctionProvider> LixFunctionProvider for SharedFunctionProvider<P> {
    fn uuid_v7(&mut self) -> String {
        self.call_uuid_v7()
    }

    fn timestamp(&mut self) -> String {
        self.call_timestamp()
    }

    fn deterministic_sequence_enabled(&self) -> bool {
        self.with_lock(|provider| provider.deterministic_sequence_enabled())
    }

    fn deterministic_sequence_initialized(&self) -> bool {
        self.with_lock(|provider| provider.deterministic_sequence_initialized())
    }

    fn initialize_deterministic_sequence(&mut self, sequence_start: i64) {
        self.with_lock_mut(|provider| provider.initialize_deterministic_sequence(sequence_start))
    }

    fn deterministic_sequence_persist_highest_seen(&self) -> Option<i64> {
        self.with_lock(|provider| provider.deterministic_sequence_persist_highest_seen())
    }
}

impl<T> LixFunctionProvider for Box<T>
where
    T: LixFunctionProvider + ?Sized,
{
    fn uuid_v7(&mut self) -> String {
        (**self).uuid_v7()
    }

    fn timestamp(&mut self) -> String {
        (**self).timestamp()
    }

    fn deterministic_sequence_enabled(&self) -> bool {
        (**self).deterministic_sequence_enabled()
    }

    fn deterministic_sequence_initialized(&self) -> bool {
        (**self).deterministic_sequence_initialized()
    }

    fn initialize_deterministic_sequence(&mut self, sequence_start: i64) {
        (**self).initialize_deterministic_sequence(sequence_start)
    }

    fn deterministic_sequence_persist_highest_seen(&self) -> Option<i64> {
        (**self).deterministic_sequence_persist_highest_seen()
    }
}
