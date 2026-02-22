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
    CanonicalLogicalViews,
    ProjectionCleanup,
    VtableRead,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueryRuleOutcome {
    NotApplicable,
    NoChange,
    Changed(Query),
}

const ANALYZE_RULES: &[QueryRule] = &[QueryRule::AnalyzeRelationDiscovery];

const CANONICALIZE_RULES: &[QueryRule] = &[QueryRule::CanonicalLogicalViews];

const OPTIMIZE_RULES: &[QueryRule] = &[QueryRule::ProjectionCleanup];

const LOWER_RULES: &[QueryRule] = &[QueryRule::VtableRead];

pub(crate) fn rules_for_phase(phase: RewritePhase) -> &'static [QueryRule] {
    match phase {
        RewritePhase::Analyze => ANALYZE_RULES,
        RewritePhase::Canonicalize => CANONICALIZE_RULES,
        RewritePhase::Optimize => OPTIMIZE_RULES,
        RewritePhase::Lower => LOWER_RULES,
    }
}

impl QueryRuleOutcome {
    fn from_option(rewritten: Option<Query>) -> Self {
        if let Some(query) = rewritten {
            Self::Changed(query)
        } else {
            Self::NoChange
        }
    }
}

impl QueryRule {
    fn apply_sync_matched(
        self,
        query: Query,
        _params: &[Value],
    ) -> Result<QueryRuleOutcome, LixError> {
        match self {
            Self::AnalyzeRelationDiscovery => {
                analyze::relation_discovery::validate_relation_discovery_consistency(&query)?;
                Ok(QueryRuleOutcome::NoChange)
            }
            Self::CanonicalLogicalViews => canonical::logical_views::rewrite_query(query, _params),
            Self::ProjectionCleanup => Ok(QueryRuleOutcome::from_option(
                optimize::projection_cleanup::rewrite_query(query)?,
            )),
            Self::VtableRead => Ok(QueryRuleOutcome::from_option(
                lower::vtable_read::rewrite_query(query)?,
            )),
        }
    }

    pub(crate) fn matches_context(self, context: &AnalysisContext) -> bool {
        match self {
            Self::AnalyzeRelationDiscovery => true,
            Self::CanonicalLogicalViews => context.references_any_logical_read_view(),
            Self::ProjectionCleanup => context.has_nested_query_shapes(),
            Self::VtableRead => context.references_relation("lix_internal_state_vtable"),
        }
    }

    #[cfg(test)]
    pub(crate) fn apply_sync(
        self,
        query: Query,
        params: &[Value],
        context: &AnalysisContext,
    ) -> Result<QueryRuleOutcome, LixError> {
        if !self.matches_context(context) {
            return Ok(QueryRuleOutcome::NotApplicable);
        }

        self.apply_sync_matched(query, params)
    }

    pub(crate) async fn apply_with_backend_and_params(
        self,
        backend: &dyn LixBackend,
        query: Query,
        params: &[Value],
        context: &AnalysisContext,
    ) -> Result<QueryRuleOutcome, LixError> {
        if !self.matches_context(context) {
            return Ok(QueryRuleOutcome::NotApplicable);
        }

        match self {
            Self::AnalyzeRelationDiscovery => self.apply_sync_matched(query, params),
            Self::CanonicalLogicalViews => {
                canonical::logical_views::rewrite_query_with_backend(backend, query, params).await
            }
            Self::ProjectionCleanup => self.apply_sync_matched(query, params),
            Self::VtableRead => Ok(QueryRuleOutcome::from_option(
                lower::vtable_read::rewrite_query_with_backend(backend, query).await?,
            )),
        }
    }
}
