use crate::LixError;

#[derive(Debug)]
pub(crate) enum ExecutorError {
    Execute(LixError),
    PostCommit {
        effect_id: String,
        attempts: usize,
        error: LixError,
    },
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
            ExecutorError::PostCommit {
                effect_id,
                attempts,
                error,
            } => LixError {
                message: format!(
                    "post-commit effect '{}' failed after {} attempt(s): {}",
                    effect_id, attempts, error.message
                ),
            },
        }
    }
}
