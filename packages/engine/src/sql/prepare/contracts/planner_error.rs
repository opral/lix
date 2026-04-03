use crate::LixError;

#[derive(Debug)]
pub(crate) enum PlannerError {
    Parse(LixError),
}

impl PlannerError {
    pub(crate) fn parse(error: LixError) -> Self {
        Self::Parse(error)
    }
}

impl From<PlannerError> for LixError {
    fn from(value: PlannerError) -> Self {
        match value {
            PlannerError::Parse(error) => error,
        }
    }
}
