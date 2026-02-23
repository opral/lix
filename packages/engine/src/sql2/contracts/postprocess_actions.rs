#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PostprocessAction {
    None,
    SqlFollowup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VtableUpdatePlan {
    pub(crate) schema_key: String,
    pub(crate) explicit_writer_key: Option<Option<String>>,
    pub(crate) writer_key_assignment_present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VtableDeletePlan {
    pub(crate) schema_key: String,
    pub(crate) effective_scope_fallback: bool,
    pub(crate) effective_scope_selection_sql: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) enum PostprocessPlan {
    VtableUpdate(VtableUpdatePlan),
    VtableDelete(VtableDeletePlan),
}
