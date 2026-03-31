use std::collections::BTreeSet;

use crate::contracts::session::SessionDependency;

/// Dependency precision communicates whether literal dependency filters are fully
/// representable by the planner.
///
/// Invariant: even in `Conservative` mode, the dependency matcher must avoid
/// false negatives. Conservative fallback may over-invalidate, but must not
/// miss a change that can affect query results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DependencyPrecision {
    Precise,
    Conservative,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DependencyWriterFilter {
    pub(crate) include: BTreeSet<String>,
    pub(crate) exclude: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DependencySpec {
    pub(crate) relations: BTreeSet<String>,
    pub(crate) schema_keys: BTreeSet<String>,
    pub(crate) entity_ids: BTreeSet<String>,
    pub(crate) file_ids: BTreeSet<String>,
    pub(crate) version_ids: BTreeSet<String>,
    pub(crate) session_dependencies: BTreeSet<SessionDependency>,
    pub(crate) writer_filter: DependencyWriterFilter,
    pub(crate) include_untracked: bool,
    pub(crate) depends_on_active_version: bool,
    pub(crate) precision: DependencyPrecision,
}

impl Default for DependencySpec {
    fn default() -> Self {
        Self {
            relations: BTreeSet::new(),
            schema_keys: BTreeSet::new(),
            entity_ids: BTreeSet::new(),
            file_ids: BTreeSet::new(),
            version_ids: BTreeSet::new(),
            session_dependencies: BTreeSet::new(),
            writer_filter: DependencyWriterFilter::default(),
            include_untracked: true,
            depends_on_active_version: false,
            precision: DependencyPrecision::Precise,
        }
    }
}
