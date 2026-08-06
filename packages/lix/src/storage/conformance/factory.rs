use crate::storage::{ReadOptions, Storage, StorageError, WriteOptions};

pub trait StorageFactory: Sync {
    type Storage: Storage;
    type Fixture: StorageFixture<Storage = Self::Storage>;

    fn create_fixture(&self) -> Self::Fixture;

    fn config(&self) -> StorageTestConfig {
        StorageTestConfig::default()
    }
}

pub trait StorageFixture: Send + Sync {
    type Storage: Storage;

    fn open(&self) -> impl Future<Output = Self::Storage> + Send;
}

pub(crate) struct OpenStorage<F>
where
    F: StorageFactory,
{
    _fixture: F::Fixture,
    storage: F::Storage,
}

pub(crate) async fn open_storage<F>(factory: &F) -> OpenStorage<F>
where
    F: StorageFactory,
{
    let fixture = factory.create_fixture();
    let storage = fixture.open().await;
    OpenStorage {
        _fixture: fixture,
        storage,
    }
}

impl<F> Storage for OpenStorage<F>
where
    F: StorageFactory,
{
    type Read<'a>
        = <F::Storage as Storage>::Read<'a>
    where
        Self: 'a;
    type Write<'a>
        = <F::Storage as Storage>::Write<'a>
    where
        Self: 'a;

    fn begin_read(
        &self,
        opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send {
        self.storage.begin_read(opts)
    }

    fn begin_write(
        &self,
        opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
        self.storage.begin_write(opts)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageTestConfig {
    pub max_key_len: usize,
    pub max_value_len: usize,
    pub ephemeral: bool,
    pub supports_concurrent_writers: bool,
}

impl Default for StorageTestConfig {
    fn default() -> Self {
        Self {
            max_key_len: 256,
            max_value_len: 4096,
            ephemeral: false,
            supports_concurrent_writers: false,
        }
    }
}
