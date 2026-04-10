use crate::catalog::SurfaceBinding;
use crate::contracts::artifacts::EffectiveStateRequest;
use crate::sql::logical_plan::dependency_spec::DependencySpec;
use crate::sql::logical_plan::direct_reads::DirectPublicReadPlan;
use crate::sql::logical_plan::public_ir::{
    BroadPublicReadStatement, PlannedWrite, StructuredPublicRead,
};
use crate::sql::logical_plan::result_contract::ResultContract;
use crate::sql::semantic_ir::internal::NormalizedInternalStatements;
use crate::sql::semantic_ir::semantics::effective_state_resolver::EffectiveStatePlan;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SurfaceReadPlan {
    pub(crate) read: StructuredPublicRead,
    pub(crate) dependency_spec: Option<DependencySpec>,
    pub(crate) effective_state_request: Option<EffectiveStateRequest>,
    pub(crate) effective_state_plan: Option<EffectiveStatePlan>,
}

impl SurfaceReadPlan {
    pub(crate) fn structured_read(&self) -> &StructuredPublicRead {
        &self.read
    }

    pub(crate) fn dependency_spec(&self) -> Option<&DependencySpec> {
        self.dependency_spec.as_ref()
    }

    pub(crate) fn effective_state_request(&self) -> Option<&EffectiveStateRequest> {
        self.effective_state_request.as_ref()
    }

    pub(crate) fn effective_state_plan(&self) -> Option<&EffectiveStatePlan> {
        self.effective_state_plan.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PublicReadLogicalPlan {
    Structured {
        plan: SurfaceReadPlan,
    },
    DirectHistory {
        plan: SurfaceReadPlan,
        direct_plan: DirectPublicReadPlan,
    },
    Broad {
        broad_statement: Box<BroadPublicReadStatement>,
        surface_bindings: Vec<SurfaceBinding>,
        dependency_spec: Option<DependencySpec>,
    },
}

impl PublicReadLogicalPlan {
    pub(crate) fn surface_read_plan(&self) -> Option<&SurfaceReadPlan> {
        match self {
            Self::Structured { plan } | Self::DirectHistory { plan, .. } => Some(plan),
            Self::Broad { .. } => None,
        }
    }

    pub(crate) fn dependency_spec(&self) -> Option<&DependencySpec> {
        self.surface_read_plan()
            .and_then(SurfaceReadPlan::dependency_spec)
            .or_else(|| match self {
                Self::Broad {
                    dependency_spec, ..
                } => dependency_spec.as_ref(),
                _ => None,
            })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn structured_read(&self) -> Option<&StructuredPublicRead> {
        self.surface_read_plan()
            .map(SurfaceReadPlan::structured_read)
    }

    pub(crate) fn effective_state_request(&self) -> Option<&EffectiveStateRequest> {
        self.surface_read_plan()
            .and_then(SurfaceReadPlan::effective_state_request)
    }

    pub(crate) fn effective_state_plan(&self) -> Option<&EffectiveStatePlan> {
        self.surface_read_plan()
            .and_then(SurfaceReadPlan::effective_state_plan)
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
