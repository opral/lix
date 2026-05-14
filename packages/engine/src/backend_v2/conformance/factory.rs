use crate::backend_v2::{Backend, BackendCapabilities};

pub trait BackendFactory {
    type Backend: Backend;

    fn fresh(&self) -> Self::Backend;

    fn capabilities(&self) -> BackendCapabilities {
        self.fresh().capabilities()
    }

    fn config(&self) -> BackendTestConfig {
        BackendTestConfig::default()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendTestConfig {
    pub max_key_len: usize,
    pub max_value_len: usize,
    pub supports_reopen: bool,
    pub supports_concurrent_writers: bool,
}

impl Default for BackendTestConfig {
    fn default() -> Self {
        Self {
            max_key_len: 256,
            max_value_len: 4096,
            supports_reopen: false,
            supports_concurrent_writers: false,
        }
    }
}
