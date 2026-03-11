use crate::LixError;

#[derive(Debug)]
pub(crate) enum PlannerError {
    Parse(LixError),
    BindOnce(LixError),
    Preprocess(LixError),
    Invariant(String),
}

impl PlannerError {
    pub(crate) fn parse(error: LixError) -> Self {
        Self::Parse(error)
    }

    pub(crate) fn bind_once(error: LixError) -> Self {
        Self::BindOnce(error)
    }

    pub(crate) fn preprocess(error: LixError) -> Self {
        Self::Preprocess(error)
    }

    pub(crate) fn invariant(message: impl Into<String>) -> Self {
        Self::Invariant(message.into())
    }
}

impl From<PlannerError> for LixError {
    fn from(value: PlannerError) -> Self {
        match value {
            PlannerError::Parse(error)
            | PlannerError::BindOnce(error)
            | PlannerError::Preprocess(error) => error,
            PlannerError::Invariant(message) => LixError::unknown(message),
        }
    }
}
