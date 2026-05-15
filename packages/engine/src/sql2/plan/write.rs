use crate::entity_identity::EntityIdentity;
use crate::sql2::bind::write::BoundWrite;
use crate::sql2::plan::predicate::FilterSet;
use crate::NullableKeyFilter;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LogicalWritePlan {
    pub(crate) bound: BoundWrite,
    pub(crate) filters: PlannedWriteFilters,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PlannedWriteFilters {
    pub(crate) schema_keys: FilterSet<String>,
    pub(crate) entity_ids: FilterSet<EntityIdentity>,
    pub(crate) version_ids: FilterSet<String>,
    pub(crate) file_ids: FilterSet<NullableKeyFilter<String>>,
}
