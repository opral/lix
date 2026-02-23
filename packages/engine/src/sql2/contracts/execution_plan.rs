use super::effects::PlanEffects;
use super::planned_statement::PlannedStatementSet;
use super::requirements::PlanRequirements;

#[derive(Debug, Clone)]
pub(crate) struct ExecutionPlan {
    pub(crate) preprocess: PlannedStatementSet,
    pub(crate) requirements: PlanRequirements,
    pub(crate) effects: PlanEffects,
    pub(crate) fingerprint: String,
}
