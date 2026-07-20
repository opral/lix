use crate::LixError;
use crate::sql2::bind::write::BoundWrite;
use crate::sql2::plan::predicate::{BoundPredicate, FilterSet};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LogicalWritePlan {
    pub(crate) bound: BoundWrite,
    pub(crate) filters: PlannedWriteFilters,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PlannedWriteFilters {
    pub(crate) rows: FilterSet,
}

pub(crate) fn plan_write(bound: BoundWrite) -> Result<LogicalWritePlan, LixError> {
    let mut filters = PlannedWriteFilters::default();
    collect_predicate_filters(&bound.predicate, &mut filters)?;

    Ok(LogicalWritePlan { bound, filters })
}

impl Default for PlannedWriteFilters {
    fn default() -> Self {
        Self {
            rows: FilterSet::All,
        }
    }
}

impl PlannedWriteFilters {
    fn set_none(&mut self) {
        self.rows = FilterSet::None;
    }
}

fn collect_predicate_filters(
    predicate: &BoundPredicate,
    filters: &mut PlannedWriteFilters,
) -> Result<(), LixError> {
    match predicate {
        BoundPredicate::True
        | BoundPredicate::Or(_)
        | BoundPredicate::Eq(_, _)
        | BoundPredicate::Like { .. }
        | BoundPredicate::IsNull(_)
        | BoundPredicate::IsNotNull(_)
        | BoundPredicate::In { .. } => Ok(()),
        BoundPredicate::False => {
            filters.set_none();
            Ok(())
        }
        BoundPredicate::And(predicates) => {
            for predicate in predicates {
                collect_predicate_filters(predicate, filters)?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql2::bind::bind_statement;
    use crate::sql2::parse_statement;
    use crate::sql2::plan::branch_scope::BranchScope;
    use std::collections::BTreeSet;

    #[test]
    fn plan_write_contradiction_does_not_drop_bound_params() {
        let plan = plan_sql(
            "UPDATE lix_state SET metadata = $1 WHERE schema_key IN ('profile') AND schema_key IN ('note') AND entity_pk = $2",
        );

        assert_eq!(
            plan.bound.params.params.keys().copied().collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn plan_write_applies_active_branch_scope_to_base_writes() {
        let plan = plan_sql("DELETE FROM lix_state WHERE schema_key = 'profile'");

        assert_eq!(
            plan.bound.branch_scope,
            BranchScope::Active {
                branch_id: "branch1".to_string()
            }
        );
    }

    #[test]
    fn plan_write_keeps_explicit_required_scope_for_by_branch_writes() {
        let plan = plan_sql("DELETE FROM lix_state_by_branch WHERE branch_id IN ('v1', 'v2')");

        assert_eq!(
            plan.bound.branch_scope,
            BranchScope::ExplicitRequired {
                branch_ids: BTreeSet::from(["v1".to_string(), "v2".to_string()])
            }
        );
    }

    #[test]
    fn plan_write_false_conjunct_sets_no_match_sentinel() {
        let plan =
            plan_sql("UPDATE lix_file SET name = 'renamed.txt' WHERE id = 'file1' AND false");

        assert_eq!(plan.filters.rows, FilterSet::None);
    }

    #[test]
    fn plan_write_user_column_names_do_not_become_storage_filters() {
        let plan = plan_sql_with_schemas(
            "UPDATE app_doc SET title = 'new' WHERE schema_key = 'draft'",
            &[serde_json::json!({
                "x-lix-key": "app_doc",
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "schema_key": { "type": "string" },
                    "title": { "type": "string" }
                },
                "x-lix-primary-key": ["/id"],
                "required": ["id", "schema_key", "title"],
                "additionalProperties": false
            })],
        );

        assert_eq!(
            plan.bound.branch_scope,
            BranchScope::Active {
                branch_id: "branch1".to_string()
            }
        );
    }

    fn plan_sql(sql: &str) -> LogicalWritePlan {
        plan_sql_with_schemas(sql, &[])
    }

    fn plan_sql_with_schemas(sql: &str, schemas: &[serde_json::Value]) -> LogicalWritePlan {
        let statement = parse_statement(sql).expect("parse SQL");
        let write = bind_statement(&statement, schemas, "branch1").expect("bind SQL");
        plan_write(write).expect("plan write")
    }
}
