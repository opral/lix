#[derive(Debug, Clone, Default)]
pub struct ExecuteOptions {
    pub writer_key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionExecutionMode {
    CommittedRead,
    CommittedRuntimeMutation,
    WriteTransaction,
}
