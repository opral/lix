use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::{LixBackend, LixError, Value};

use super::super::ast::nodes::Statement;
use super::super::contracts::effects::DetectedFileDomainChange;
use super::super::contracts::planned_statement::PlannedStatementSet;
use super::super::planning::preprocess::preprocess_with_surfaces_to_plan;
use super::super::vtable;
use super::{entity, filesystem, lix_state, lix_state_by_version, lix_state_history};

pub(crate) type DetectedFileDomainChangesByStatement = [Vec<DetectedFileDomainChange>];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SurfaceKind {
    Vtable,
    LixState,
    LixStateByVersion,
    LixStateHistory,
    Filesystem,
    Entity,
    Generic,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SurfaceCoverage {
    pub(crate) vtable: usize,
    pub(crate) lix_state: usize,
    pub(crate) lix_state_by_version: usize,
    pub(crate) lix_state_history: usize,
    pub(crate) filesystem: usize,
    pub(crate) entity: usize,
    pub(crate) generic: usize,
}

pub(crate) fn classify_statement(statement: &Statement) -> SurfaceKind {
    if vtable::registry::detect_registered_vtable(statement).is_some() {
        return SurfaceKind::Vtable;
    }
    if lix_state_by_version::planner::matches(statement) {
        return SurfaceKind::LixStateByVersion;
    }
    if lix_state_history::planner::matches(statement) {
        return SurfaceKind::LixStateHistory;
    }
    if lix_state::planner::matches(statement) {
        return SurfaceKind::LixState;
    }
    if filesystem::planner::matches(statement) {
        return SurfaceKind::Filesystem;
    }
    if entity::planner::matches(statement) {
        return SurfaceKind::Entity;
    }
    SurfaceKind::Generic
}

pub(crate) fn collect_surface_coverage(statements: &[Statement]) -> SurfaceCoverage {
    let mut coverage = SurfaceCoverage::default();
    for statement in statements {
        match classify_statement(statement) {
            SurfaceKind::Vtable => {
                let _ = vtable::registry::capabilities_for_statement(statement);
                let _ =
                    vtable::internal_state_vtable::lower_read::supports_internal_state_vtable_read(
                        statement,
                    );
                let _ = vtable::internal_state_vtable::lower_write::supports_internal_state_vtable_write(statement);
                coverage.vtable += 1;
            }
            SurfaceKind::LixState => {
                let _ = lix_state::lower::lowering_kind(statement);
                coverage.lix_state += 1;
            }
            SurfaceKind::LixStateByVersion => {
                let _ = lix_state_by_version::lower::lowering_kind(statement);
                coverage.lix_state_by_version += 1;
            }
            SurfaceKind::LixStateHistory => {
                let _ = lix_state_history::lower::lowering_kind(statement);
                coverage.lix_state_history += 1;
            }
            SurfaceKind::Filesystem => {
                let _ = filesystem::lower::lowering_kind(statement);
                coverage.filesystem += 1;
            }
            SurfaceKind::Entity => {
                let _ = entity::lower::lowering_kind(statement);
                coverage.entity += 1;
            }
            SurfaceKind::Generic => {
                coverage.generic += 1;
            }
        }
    }
    coverage
}

pub(crate) async fn preprocess_with_surfaces<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    statements: Vec<Statement>,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    detected_file_domain_changes_by_statement: &DetectedFileDomainChangesByStatement,
    writer_key: Option<&str>,
) -> Result<PlannedStatementSet, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let _coverage = collect_surface_coverage(&statements);
    preprocess_with_surfaces_to_plan(
        backend,
        evaluator,
        statements,
        params,
        functions,
        detected_file_domain_changes_by_statement,
        writer_key,
    )
    .await
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::{classify_statement, SurfaceKind};

    #[test]
    fn classifies_state_by_version_before_state() {
        let statements = Parser::parse_sql(
            &GenericDialect {},
            "SELECT * FROM lix_state_by_version WHERE version_id = 'v1'",
        )
        .expect("parse SQL");
        assert_eq!(
            classify_statement(&statements[0]),
            SurfaceKind::LixStateByVersion
        );
    }

    #[test]
    fn classifies_filesystem_surface() {
        let statements =
            Parser::parse_sql(&GenericDialect {}, "SELECT * FROM lix_file WHERE id = 'f'")
                .expect("parse SQL");
        assert_eq!(classify_statement(&statements[0]), SurfaceKind::Filesystem);
    }

    #[test]
    fn classifies_internal_state_vtable_surface() {
        let statements = Parser::parse_sql(
            &GenericDialect {},
            "SELECT * FROM lix_internal_state_vtable WHERE schema_key = 'x'",
        )
        .expect("parse SQL");
        assert_eq!(classify_statement(&statements[0]), SurfaceKind::Vtable);
    }
}
