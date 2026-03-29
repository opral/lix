use crate::sql::catalog::SurfaceBinding;
use crate::sql::logical_plan::dependency_spec::DependencySpec;
use crate::sql::logical_plan::direct_reads::DirectPublicReadPlan;
use crate::sql::logical_plan::public_ir::{PlannedWrite, StructuredPublicRead};
use crate::sql::logical_plan::result_contract::ResultContract;
use crate::sql::semantic_ir::internal::NormalizedInternalStatements;
use crate::sql::semantic_ir::semantics::effective_state_resolver::{
    EffectiveStatePlan, EffectiveStateRequest,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PublicReadLogicalPlan {
    Structured {
        read: StructuredPublicRead,
        dependency_spec: Option<DependencySpec>,
        effective_state_request: Option<EffectiveStateRequest>,
        effective_state_plan: Option<EffectiveStatePlan>,
    },
    DirectHistory {
        read: StructuredPublicRead,
        direct_plan: DirectPublicReadPlan,
        dependency_spec: Option<DependencySpec>,
        effective_state_request: Option<EffectiveStateRequest>,
        effective_state_plan: Option<EffectiveStatePlan>,
    },
    Broad {
        surface_bindings: Vec<SurfaceBinding>,
        dependency_spec: Option<DependencySpec>,
    },
}

impl PublicReadLogicalPlan {
    pub(crate) fn dependency_spec(&self) -> Option<&DependencySpec> {
        match self {
            Self::Structured {
                dependency_spec, ..
            }
            | Self::DirectHistory {
                dependency_spec, ..
            }
            | Self::Broad {
                dependency_spec, ..
            } => dependency_spec.as_ref(),
        }
    }

    pub(crate) fn structured_read(&self) -> Option<&StructuredPublicRead> {
        match self {
            Self::Structured { read, .. } | Self::DirectHistory { read, .. } => Some(read),
            Self::Broad { .. } => None,
        }
    }

    pub(crate) fn effective_state_request(&self) -> Option<&EffectiveStateRequest> {
        match self {
            Self::Structured {
                effective_state_request,
                ..
            }
            | Self::DirectHistory {
                effective_state_request,
                ..
            } => effective_state_request.as_ref(),
            Self::Broad { .. } => None,
        }
    }

    pub(crate) fn effective_state_plan(&self) -> Option<&EffectiveStatePlan> {
        match self {
            Self::Structured {
                effective_state_plan,
                ..
            }
            | Self::DirectHistory {
                effective_state_plan,
                ..
            } => effective_state_plan.as_ref(),
            Self::Broad { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PublicWriteLogicalPlan {
    pub(crate) planned_write: PlannedWrite,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InternalLogicalPlan {
    pub(crate) normalized_statements: NormalizedInternalStatements,
    pub(crate) result_contract: ResultContract,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LogicalPlan {
    PublicRead(PublicReadLogicalPlan),
    PublicWrite(PublicWriteLogicalPlan),
    Internal(InternalLogicalPlan),
}
