use super::super::contracts::requirements::PlanRequirements;
use super::super::semantics::state_resolution::requirements::derive_requirements_from_state_resolution;
use sqlparser::ast::Statement;

pub(crate) fn derive_plan_requirements(statements: &[Statement]) -> PlanRequirements {
    derive_requirements_from_state_resolution(statements)
}
