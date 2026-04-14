use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default)]
pub struct ExecuteOptions {
    pub writer_key: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SessionDependency {
    ActiveVersion,
    ActiveAccounts,
    PublicSurfaceRegistryGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionExecutionMode {
    CommittedRead,
    CommittedRuntimeMutation,
    WriteTransaction,
}
