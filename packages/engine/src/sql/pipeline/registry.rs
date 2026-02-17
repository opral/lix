use sqlparser::ast::Query;

use crate::{LixBackend, LixError, Value};

use super::context::AnalysisContext;
use super::rules::query::{analyze, canonical, lower, optimize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RewritePhase {
    Analyze,
    Canonicalize,
    Optimize,
    Lower,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryRule {
    AnalyzeRelationDiscovery,
    FilesystemViews,
    EntityViews,
    LixVersion,
    LixActiveAccount,
    LixActiveVersion,

    LixState,
    LixStateByVersion,
    LixStateHistory,
    Pushdown,
    ProjectionCleanup,
    VtableRead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatementRule {
    QueryRead,
    ExplainRead,
    VtableWriteCanonical,
    Passthrough,
}

const ANALYZE_RULES: &[QueryRule] = &[QueryRule::AnalyzeRelationDiscovery];

const CANONICALIZE_RULES: &[QueryRule] = &[
    QueryRule::FilesystemViews,
    QueryRule::EntityViews,
    QueryRule::LixVersion,
    QueryRule::LixActiveAccount,
    QueryRule::LixActiveVersion,
    QueryRule::LixState,
    QueryRule::LixStateByVersion,
    QueryRule::LixStateHistory,
];

const OPTIMIZE_RULES: &[QueryRule] = &[QueryRule::Pushdown, QueryRule::ProjectionCleanup];

const LOWER_RULES: &[QueryRule] = &[QueryRule::VtableRead];

const STATEMENT_RULES: &[StatementRule] = &[
    StatementRule::QueryRead,
    StatementRule::ExplainRead,
    StatementRule::VtableWriteCanonical,
    StatementRule::Passthrough,
];

pub(crate) fn rules_for_phase(phase: RewritePhase) -> &'static [QueryRule] {
    match phase {
        RewritePhase::Analyze => ANALYZE_RULES,
        RewritePhase::Canonicalize => CANONICALIZE_RULES,
        RewritePhase::Optimize => OPTIMIZE_RULES,
        RewritePhase::Lower => LOWER_RULES,
    }
}

pub(crate) fn statement_rules() -> &'static [StatementRule] {
    STATEMENT_RULES
}

impl QueryRule {
    pub(crate) fn matches_context(self, context: &AnalysisContext) -> bool {
        match self {
            Self::AnalyzeRelationDiscovery => true,
            Self::FilesystemViews => context.references_any_filesystem_view(),
            Self::EntityViews => context.references_entity_views(),
            Self::LixVersion => context.references_relation("lix_version"),
            Self::LixActiveAccount => context.references_relation("lix_active_account"),
            Self::LixActiveVersion => context.references_relation("lix_active_version"),
            Self::LixState => context.references_relation("lix_state"),
            Self::LixStateByVersion => context.references_relation("lix_state_by_version"),
            Self::LixStateHistory => context.references_relation("lix_state_history"),
            Self::Pushdown => context.references_state_views(),
            Self::ProjectionCleanup => context.has_nested_query_shapes(),
            Self::VtableRead => context.references_relation("lix_internal_state_vtable"),
        }
    }

    pub(crate) fn apply_sync(
        self,
        query: Query,
        params: &[Value],
    ) -> Result<Option<Query>, LixError> {
        match self {
            Self::AnalyzeRelationDiscovery => {
                analyze::relation_discovery::validate_relation_discovery_consistency(&query)?;
                Ok(None)
            }
            Self::FilesystemViews => canonical::filesystem_views::rewrite_query(query, params),
            Self::EntityViews => canonical::entity_views::rewrite_query(query),
            Self::LixVersion => canonical::lix_version::rewrite_query(query),
            Self::LixActiveAccount => canonical::lix_active_account::rewrite_query(query),
            Self::LixActiveVersion => canonical::lix_active_version::rewrite_query(query),
            Self::LixState => canonical::lix_state::rewrite_query(query),
            Self::LixStateByVersion => canonical::lix_state_by_version::rewrite_query(query),
            Self::LixStateHistory => canonical::lix_state_history::rewrite_query(query),
            Self::Pushdown => optimize::pushdown::rewrite_query(query),
            Self::ProjectionCleanup => optimize::projection_cleanup::rewrite_query(query),
            Self::VtableRead => lower::vtable_read::rewrite_query(query),
        }
    }

    pub(crate) async fn apply_with_backend_and_params(
        self,
        backend: &dyn LixBackend,
        query: Query,
        params: &[Value],
    ) -> Result<Option<Query>, LixError> {
        match self {
            Self::AnalyzeRelationDiscovery => {
                analyze::relation_discovery::validate_relation_discovery_consistency(&query)?;
                Ok(None)
            }
            Self::FilesystemViews => canonical::filesystem_views::rewrite_query(query, params),
            Self::EntityViews => {
                canonical::entity_views::rewrite_query_with_backend(backend, query).await
            }
            Self::LixVersion => canonical::lix_version::rewrite_query(query),
            Self::LixActiveAccount => canonical::lix_active_account::rewrite_query(query),
            Self::LixActiveVersion => canonical::lix_active_version::rewrite_query(query),
            Self::LixState => canonical::lix_state::rewrite_query(query),
            Self::LixStateByVersion => canonical::lix_state_by_version::rewrite_query(query),
            Self::LixStateHistory => canonical::lix_state_history::rewrite_query(query),
            Self::Pushdown => optimize::pushdown::rewrite_query(query),
            Self::ProjectionCleanup => optimize::projection_cleanup::rewrite_query(query),
            Self::VtableRead => {
                lower::vtable_read::rewrite_query_with_backend(backend, query).await
            }
        }
    }
}
