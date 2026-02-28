use super::effects::PlanEffects;
use super::planned_statement::PlannedStatementSet;
use super::requirements::PlanRequirements;
use super::result_contract::ResultContract;

#[derive(Debug, Clone)]
pub(crate) struct ExecutionPlan {
    pub(crate) preprocess: PlannedStatementSet,
    pub(crate) result_contract: ResultContract,
    pub(crate) requirements: PlanRequirements,
    pub(crate) effects: PlanEffects,
}
