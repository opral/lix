use crate::sql::analysis::state_resolution::requirements::derive_requirements_from_state_resolution;
use crate::sql::execution::contracts::requirements::PlanRequirements;
use sqlparser::ast::Statement;

pub(crate) fn derive_plan_requirements(statements: &[Statement]) -> PlanRequirements {
    derive_requirements_from_state_resolution(statements)
}
