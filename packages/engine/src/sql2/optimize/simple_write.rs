use crate::sql2::bind::write::{BoundWriteOp, BoundWriteTarget};
use crate::sql2::plan::predicate::FilterSet;
use crate::sql2::plan::version_scope::VersionScope;
use crate::sql2::plan::LogicalWritePlan;
use crate::LixError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FastWritePlan {
    Update(FastUpdatePlan),
    Delete(FastDeletePlan),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FastUpdatePlan;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FastDeletePlan;

pub(crate) fn try_make_fast_write_plan(
    plan: &LogicalWritePlan,
) -> Result<Option<FastWritePlan>, LixError> {
    if !is_supported_fast_target(plan) || !is_known_no_match(plan) {
        return Ok(None);
    }

    Ok(match plan.bound.op {
        BoundWriteOp::Insert => None,
        BoundWriteOp::Update => Some(FastWritePlan::Update(FastUpdatePlan)),
        BoundWriteOp::Delete => Some(FastWritePlan::Delete(FastDeletePlan)),
    })
}

fn is_supported_fast_target(plan: &LogicalWritePlan) -> bool {
    matches!(
        plan.bound.target,
        BoundWriteTarget::LixState | BoundWriteTarget::LixStateByVersion
    )
}

fn is_known_no_match(plan: &LogicalWritePlan) -> bool {
    matches!(plan.bound.version_scope, VersionScope::Empty)
        || matches!(plan.filters.rows, FilterSet::None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql2::bind::bind_statement;
    use crate::sql2::parse_statement;
    use crate::sql2::plan::plan_write;

    #[test]
    fn try_make_fast_write_plan_declines_column_contradictions() {
        let plan = plan_sql(
            "UPDATE lix_state SET metadata = '{}' \
             WHERE schema_key = 'profile' AND schema_key = 'note'",
        );

        assert_eq!(
            try_make_fast_write_plan(&plan).expect("optimization should not fail"),
            None
        );
    }

    #[test]
    fn try_make_fast_write_plan_accepts_false_delete_as_noop() {
        let plan = plan_sql("DELETE FROM lix_state WHERE false");

        assert_eq!(
            try_make_fast_write_plan(&plan).expect("optimization should not fail"),
            Some(FastWritePlan::Delete(FastDeletePlan))
        );
    }

    #[test]
    fn try_make_fast_write_plan_declines_complex_update() {
        let plan = plan_sql(
            "UPDATE lix_state SET metadata = lix_json('{\"schema_key\":\"lix_key_value\"}') \
             WHERE metadata = lix_json('{\"source\":\"match\"}')",
        );

        assert_eq!(
            try_make_fast_write_plan(&plan).expect("optimization should not fail"),
            None
        );
    }

    #[test]
    fn try_make_fast_write_plan_declines_literal_insert() {
        let plan = plan_sql(
            "INSERT INTO lix_state (entity_id, schema_key, snapshot_content) \
             VALUES (lix_json('[\"entity-1\"]'), 'lix_key_value', '{}')",
        );

        assert_eq!(
            try_make_fast_write_plan(&plan).expect("optimization should not fail"),
            None
        );
    }

    #[test]
    fn try_make_fast_write_plan_declines_unsupported_targets_even_when_no_match() {
        let plan = plan_sql("DELETE FROM lix_version WHERE false");

        assert_eq!(
            try_make_fast_write_plan(&plan).expect("optimization should not fail"),
            None
        );
    }

    fn plan_sql(sql: &str) -> LogicalWritePlan {
        let statement = parse_statement(sql).expect("SQL parses");
        let write = bind_statement(&statement, &[], "version1").expect("SQL binds");
        plan_write(write).expect("write plans")
    }
}
