use crate::sql2::planner::ir::WriteLane;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SemanticEffect {
    pub(crate) effect_key: String,
    pub(crate) target: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DomainChangeBatch {
    pub(crate) change_ids: Vec<String>,
    pub(crate) write_lane: WriteLane,
    pub(crate) writer_key: Option<String>,
    pub(crate) semantic_effects: Vec<SemanticEffect>,
}
