use crate::backend_v2::{Backend, BackendCapabilities, BackendError, ReadOptions, WriteOptions};

pub trait BackendFactory {
    type Backend: Backend;
    type Fixture: BackendFixture<Backend = Self::Backend>;

    fn create_fixture(&self) -> Self::Fixture;

    fn capabilities(&self) -> BackendCapabilities {
        self.create_fixture().open().capabilities()
    }

    fn config(&self) -> BackendTestConfig {
        BackendTestConfig::default()
    }
}

pub trait BackendFixture {
    type Backend: Backend;

    fn open(&self) -> Self::Backend;
}

pub(crate) struct OpenBackend<F>
where
    F: BackendFactory,
{
    _fixture: F::Fixture,
    backend: F::Backend,
}

pub(crate) fn open_backend<F>(factory: &F) -> OpenBackend<F>
where
    F: BackendFactory,
{
    let fixture = factory.create_fixture();
    let backend = fixture.open();
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

    fn capabilities(&self) -> BackendCapabilities {
        self.backend.capabilities()
    }

    fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        self.backend.begin_read(opts)
    }

    fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        self.backend.begin_write(opts)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
