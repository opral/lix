use crate::LixError;

#[derive(Debug)]
pub(crate) enum ExecutorError {
    Execute(LixError),
}

impl ExecutorError {
    pub(crate) fn execute(error: LixError) -> Self {
        Self::Execute(error)
    }
}

impl From<LixError> for ExecutorError {
    fn from(value: LixError) -> Self {
        Self::Execute(value)
    }
}

impl From<ExecutorError> for LixError {
    fn from(value: ExecutorError) -> Self {
        match value {
            ExecutorError::Execute(error) => error,
        }
    }
}
