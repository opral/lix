use crate::backend::{BackendError, BackendRead};

pub trait StorageRead {
    type BackendRead: BackendRead;

    fn backend_read(&self) -> &Self::BackendRead;
}

#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct StorageReadScope<R> {
    read: R,
}

impl<R> StorageReadScope<R> {
    pub fn new(read: R) -> Self {
        Self { read }
    }

    pub fn store(&self) -> Self
    where
        R: Clone,
    {
        self.clone()
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

    fn backend_read(&self) -> &Self::BackendRead {
        &self.read
    }
}

impl<T> StorageRead for &T
where
    T: StorageRead + ?Sized,
{
    type BackendRead = T::BackendRead;

    fn backend_read(&self) -> &Self::BackendRead {
        (*self).backend_read()
    }
}

impl<T> StorageRead for &mut T
where
    T: StorageRead + ?Sized,
{
    type BackendRead = T::BackendRead;

    fn backend_read(&self) -> &Self::BackendRead {
        (**self).backend_read()
    }
}
