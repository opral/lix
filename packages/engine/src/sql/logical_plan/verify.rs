use crate::sql::logical_plan::direct_reads::{
    DirectPublicReadPlan, DirectoryHistoryDirectReadPlan, FileHistoryDirectReadPlan,
    StateHistoryDirectReadPlan,
};
use crate::sql::logical_plan::plan::{
    InternalLogicalPlan, LogicalPlan, PublicReadLogicalPlan, PublicWriteLogicalPlan,
};
use crate::sql::logical_plan::result_contract::ResultContract;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogicalPlanVerificationError {
    pub(crate) message: String,
}

impl LogicalPlanVerificationError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub(crate) fn verify_logical_plan(plan: &LogicalPlan) -> Result<(), LogicalPlanVerificationError> {
    match plan {
        LogicalPlan::PublicRead(plan) => verify_public_read_logical_plan(plan),
        LogicalPlan::PublicWrite(plan) => verify_public_write_logical_plan(plan),
        LogicalPlan::Internal(plan) => verify_internal_logical_plan(plan),
    }
}

pub(crate) fn verify_public_read_logical_plan(
    plan: &PublicReadLogicalPlan,
) -> Result<(), LogicalPlanVerificationError> {
    match plan {
        PublicReadLogicalPlan::Structured { read, .. } => {
            if read.surface_binding.descriptor.public_name.is_empty() {
                return Err(LogicalPlanVerificationError::new(
                    "structured public read must target a named surface",
                ));
            }
        }
        PublicReadLogicalPlan::DirectHistory {
            read, direct_plan, ..
        } => {
            if read.surface_binding.descriptor.public_name.is_empty() {
                return Err(LogicalPlanVerificationError::new(
                    "direct history read must target a named surface",
                ));
            }
            verify_direct_public_read_plan(direct_plan)?;
        }
        PublicReadLogicalPlan::Broad {
            surface_bindings, ..
        } => {
            if surface_bindings.is_empty() {
                return Err(LogicalPlanVerificationError::new(
                    "broad public read logical plan must record at least one bound surface",
                ));
            }
        }
    }

    Ok(())
}

pub(crate) fn verify_public_write_logical_plan(
    plan: &PublicWriteLogicalPlan,
) -> Result<(), LogicalPlanVerificationError> {
    if plan
        .planned_write
        .command
        .target
        .descriptor
        .public_name
        .is_empty()
    {
        return Err(LogicalPlanVerificationError::new(
            "planned write must target a named surface",
        ));
    }

    Ok(())
}

pub(crate) fn verify_internal_logical_plan(
    plan: &InternalLogicalPlan,
) -> Result<(), LogicalPlanVerificationError> {
    if plan.normalized_statements.prepared_statements.is_empty()
        && !matches!(plan.result_contract, ResultContract::DmlNoReturning)
        && plan.normalized_statements.mutations.is_empty()
        && plan.normalized_statements.update_validations.is_empty()
    {
        return Err(LogicalPlanVerificationError::new(
            "internal logical plan must contain statements or explicit internal effects",
        ));
    }

    Ok(())
}

fn verify_direct_public_read_plan(
    plan: &DirectPublicReadPlan,
) -> Result<(), LogicalPlanVerificationError> {
    match plan {
        DirectPublicReadPlan::StateHistory(plan) => verify_state_history_direct_plan(plan),
        DirectPublicReadPlan::EntityHistory(plan) => {
            if plan.surface_binding.descriptor.public_name.is_empty() {
                Err(LogicalPlanVerificationError::new(
                    "entity history direct read must target a named surface",
                ))
            } else {
                Ok(())
            }
        }
        DirectPublicReadPlan::FileHistory(plan) => verify_file_history_direct_plan(plan),
        DirectPublicReadPlan::DirectoryHistory(plan) => verify_directory_history_direct_plan(plan),
    }
}

fn verify_state_history_direct_plan(
    plan: &StateHistoryDirectReadPlan,
) -> Result<(), LogicalPlanVerificationError> {
    if plan.having.is_some() && plan.group_by_fields.is_empty() && plan.projections.is_empty() {
        return Err(LogicalPlanVerificationError::new(
            "state history aggregate predicates require grouped or projected inputs",
        ));
    }

    Ok(())
}

fn verify_file_history_direct_plan(
    plan: &FileHistoryDirectReadPlan,
) -> Result<(), LogicalPlanVerificationError> {
    match (&plan.aggregate, &plan.aggregate_output_name) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        (Some(_), None) => Err(LogicalPlanVerificationError::new(
            "file history aggregate requires an output name",
        )),
        (None, Some(_)) => Err(LogicalPlanVerificationError::new(
            "file history aggregate output name requires an aggregate",
        )),
    }
}

fn verify_directory_history_direct_plan(
    plan: &DirectoryHistoryDirectReadPlan,
) -> Result<(), LogicalPlanVerificationError> {
    match (&plan.aggregate, &plan.aggregate_output_name) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        (Some(_), None) => Err(LogicalPlanVerificationError::new(
            "directory history aggregate requires an output name",
        )),
        (None, Some(_)) => Err(LogicalPlanVerificationError::new(
            "directory history aggregate output name requires an aggregate",
        )),
    }
}
