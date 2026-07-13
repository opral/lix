use crate::backend::{Backend, BackendError, ReadOptions, WriteOptions};

pub trait BackendFactory: Sync {
    type Backend: Backend;
    type Fixture: BackendFixture<Backend = Self::Backend>;

    fn create_fixture(&self) -> Self::Fixture;

    fn config(&self) -> BackendTestConfig {
        BackendTestConfig::default()
    }
}

pub trait BackendFixture: Send + Sync {
    type Backend: Backend;

    fn open(&self) -> impl Future<Output = Self::Backend> + Send;
}

pub(crate) struct OpenBackend<F>
where
    F: BackendFactory,
{
    _fixture: F::Fixture,
    backend: F::Backend,
}

pub(crate) async fn open_backend<F>(factory: &F) -> OpenBackend<F>
where
    F: BackendFactory,
{
    let fixture = factory.create_fixture();
    let backend = fixture.open().await;
    OpenBackend {
        _fixture: fixture,
        backend,
    }
}

impl<F> Backend for OpenBackend<F>
where
    F: BackendFactory,
{
    type Read<'a>
        = <F::Backend as Backend>::Read<'a>
    where
        Self: 'a;
    type Write<'a>
        = <F::Backend as Backend>::Write<'a>
    where
        Self: 'a;

    fn begin_read(
        &self,
        opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, BackendError>> + Send {
        self.backend.begin_read(opts)
    }

    fn begin_write(
        &self,
        opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, BackendError>> + Send {
        self.backend.begin_write(opts)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackendTestConfig {
    pub max_key_len: usize,
    pub max_value_len: usize,
    pub ephemeral: bool,
    pub supports_concurrent_writers: bool,
}

impl Default for BackendTestConfig {
    fn default() -> Self {
        Self {
            max_key_len: 256,
            max_value_len: 4096,
            ephemeral: false,
            supports_concurrent_writers: false,
        }
    }
}
