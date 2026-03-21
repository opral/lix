#[derive(Debug, Clone, Default)]
pub(crate) struct PlanRequirements {
    pub(crate) read_only_query: bool,
    pub(crate) should_refresh_file_cache: bool,
}
