use crate::backend_v2::{BackendError, BackendRead};

pub trait StorageRead {
    type BackendRead: BackendRead;

    fn backend_read(&self) -> &Self::BackendRead;
}

pub struct StorageReadScope<R> {
    read: R,
}

impl<R> StorageReadScope<R> {
    pub fn new(read: R) -> Self {
        Self { read }
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
