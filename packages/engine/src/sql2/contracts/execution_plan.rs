use crate::sql::PreprocessOutput;

use super::effects::PlanEffects;
use super::requirements::PlanRequirements;

#[derive(Debug, Clone)]
pub(crate) struct ExecutionPlan {
    pub(crate) preprocess: PreprocessOutput,
    pub(crate) requirements: PlanRequirements,
    pub(crate) effects: PlanEffects,
    pub(crate) fingerprint: String,
}
