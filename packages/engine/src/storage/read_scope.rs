use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::backend::{BackendError, BackendRead};

thread_local! {
    static ACTIVE_SHARED_READS: RefCell<HashSet<usize>> = RefCell::new(HashSet::new());
}

pub trait StorageRead {
    type BackendRead: BackendRead;

    fn with_backend<T>(
        &self,
        f: impl FnOnce(&Self::BackendRead) -> Result<T, BackendError>,
    ) -> Result<T, BackendError>;
}

#[expect(missing_debug_implementations)]
pub struct StorageReadScope<R> {
    read: R,
}

impl<R> StorageReadScope<R> {
    pub fn new(read: R) -> Self {
        Self { read }
    }
}

/// Cloneable SQL/DataFusion read bridge for one execution-scoped backend read.
///
/// This is deliberately not a parallel-read abstraction. All clones share one
/// backend read and `with_backend()` holds a mutex for the whole backend call,
/// including scan callbacks, so provider clones are serialized through the
/// original read scope.
pub(crate) struct SharedStorageRead<R>
where
    R: BackendRead,
{
    state: Arc<Mutex<SharedStorageReadState<R>>>,
}

struct SharedStorageReadState<R>
where
    R: BackendRead,
{
    read: Option<StorageReadScope<R>>,
}

impl<R> SharedStorageRead<R>
where
    R: BackendRead,
{
    pub(crate) fn new(read: StorageReadScope<R>) -> Self {
        Self {
            state: Arc::new(Mutex::new(SharedStorageReadState { read: Some(read) })),
        }
    }

    pub(crate) fn close(self) -> Result<(), BackendError> {
        let strong_count = Arc::strong_count(&self.state);
        if strong_count > 1 {
            return Err(BackendError::Io(format!(
                "shared storage read still has {} active handles",
                strong_count - 1
            )));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|error| BackendError::Io(format!("shared storage read poisoned: {error}")))?;
        let Some(read) = state.read.take() else {
            return Ok(());
        };
        drop(state);
        read.close()
    }
}

impl<R> Clone for SharedStorageRead<R>
where
    R: BackendRead,
{
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<R> Drop for SharedStorageRead<R>
where
    R: BackendRead,
{
    fn drop(&mut self) {
        if Arc::strong_count(&self.state) == 1 {
            let Some(read) = self
                .state
                .lock()
                .ok()
                .and_then(|mut state| state.read.take())
            else {
                return;
            };
            let _ = read.close();
        }
    }
}

impl<R> StorageRead for SharedStorageRead<R>
where
    R: BackendRead,
{
    type BackendRead = R;

    fn with_backend<T>(
        &self,
        f: impl FnOnce(&Self::BackendRead) -> Result<T, BackendError>,
    ) -> Result<T, BackendError> {
        // This bridge serializes access to one backend read scope. The mutex is
        // intentionally held while `f` runs because `f` may open a streaming
        // backend scan over borrowed state.
        let _active = SharedReadActiveGuard::enter(Arc::as_ptr(&self.state).cast::<()>())?;
        let state = self
            .state
            .lock()
            .map_err(|error| BackendError::Io(format!("shared storage read poisoned: {error}")))?;
        let Some(read) = &state.read else {
            return Err(BackendError::Io(
                "shared storage read is closed".to_string(),
            ));
        };
        f(&read.read)
    }
}

struct SharedReadActiveGuard {
    key: usize,
}

impl SharedReadActiveGuard {
    fn enter(ptr: *const ()) -> Result<Self, BackendError> {
        let key = ptr as usize;
        ACTIVE_SHARED_READS.with(|active| {
            let mut active = active.borrow_mut();
            if !active.insert(key) {
                return Err(BackendError::Io(
                    "shared storage read re-entered from the same thread".to_string(),
                ));
            }
            Ok(Self { key })
        })
    }
}

impl Drop for SharedReadActiveGuard {
    fn drop(&mut self) {
        ACTIVE_SHARED_READS.with(|active| {
            active.borrow_mut().remove(&self.key);
        });
    }
}

impl<R> StorageReadScope<R>
where
    R: BackendRead,
{
    pub fn close(self) -> Result<(), BackendError> {
        self.read.close()
    }
}

impl<R> StorageRead for StorageReadScope<R>
where
    R: BackendRead,
{
    type BackendRead = R;

    fn with_backend<T>(
        &self,
        f: impl FnOnce(&Self::BackendRead) -> Result<T, BackendError>,
    ) -> Result<T, BackendError> {
        f(&self.read)
    }
}

impl<T> StorageRead for &T
where
    T: StorageRead + ?Sized,
{
    type BackendRead = T::BackendRead;

    fn with_backend<U>(
        &self,
        f: impl FnOnce(&Self::BackendRead) -> Result<U, BackendError>,
    ) -> Result<U, BackendError> {
        (*self).with_backend(f)
    }
}

impl<T> StorageRead for &mut T
where
    T: StorageRead + ?Sized,
{
    type BackendRead = T::BackendRead;

    fn with_backend<U>(
        &self,
        f: impl FnOnce(&Self::BackendRead) -> Result<U, BackendError>,
    ) -> Result<U, BackendError> {
        (**self).with_backend(f)
    }
}
