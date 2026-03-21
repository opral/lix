use crate::LixError;

#[derive(Debug)]
pub(crate) enum PlannerError {
    Parse(LixError),
    Preprocess(LixError),
}

impl PlannerError {
    pub(crate) fn parse(error: LixError) -> Self {
        Self::Parse(error)
    }

    pub(crate) fn preprocess(error: LixError) -> Self {
        Self::Preprocess(error)
    }
}

impl From<PlannerError> for LixError {
    fn from(value: PlannerError) -> Self {
        match value {
            PlannerError::Parse(error) | PlannerError::Preprocess(error) => error,
        }
    }
}
