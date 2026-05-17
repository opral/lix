use crate::sql2::bind::expr::{BoundExpr, BoundLiteral};
use crate::sql2::bind::write::BoundWrite;
use crate::sql2::plan::predicate::{BoundPredicate, FilterSet};
use crate::LixError;
use std::collections::BTreeSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LogicalWritePlan {
    pub(crate) bound: BoundWrite,
    pub(crate) filters: PlannedWriteFilters,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PlannedWriteFilters {
    pub(crate) rows: FilterSet<()>,
    pub(crate) columns: Vec<PlannedColumnFilter>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PlannedColumnFilter {
    pub(crate) column: crate::sql2::bind::expr::BoundColumnRef,
    pub(crate) values: FilterSet<PlannedValue>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum PlannedValue {
    Bool(bool),
    Integer(i64),
    Text(String),
    Json(String),
    Blob(Vec<u8>),
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
            columns: Vec::new(),
        }
    }
}

impl PlannedWriteFilters {
    fn set_none(&mut self) {
        self.rows = FilterSet::None;
        for filter in &mut self.columns {
            filter.values = FilterSet::None;
        }
    }
}

impl<T: Clone + Ord> FilterSet<T> {
    fn intersect_with(&mut self, other: Self) {
        *self = match (std::mem::replace(self, Self::All), other) {
            (Self::None, _) | (_, Self::None) => Self::None,
            (Self::All, set) | (set, Self::All) => set,
            (Self::Some(left), Self::Some(right)) => {
                let values = left.intersection(&right).cloned().collect::<BTreeSet<_>>();
                if values.is_empty() {
                    Self::None
                } else {
                    Self::Some(values)
                }
            }
        };
    }
}

fn collect_predicate_filters(
    predicate: &BoundPredicate,
    filters: &mut PlannedWriteFilters,
) -> Result<(), LixError> {
    match predicate {
        BoundPredicate::True => Ok(()),
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
        BoundPredicate::Or(_) => Ok(()),
        BoundPredicate::Eq(left, right) => {
            apply_binary_filter(left, right, filters)?;
            apply_binary_filter(right, left, filters)
        }
        BoundPredicate::In { expr, values } => {
            let BoundExpr::Column(column) = expr else {
                return Ok(());
            };
            apply_column_values_filter(column, values, filters)
        }
    }
}

fn apply_binary_filter(
    column_expr: &BoundExpr,
    value_expr: &BoundExpr,
    filters: &mut PlannedWriteFilters,
) -> Result<(), LixError> {
    let BoundExpr::Column(column) = column_expr else {
        return Ok(());
    };
    apply_column_values_filter(column, std::slice::from_ref(value_expr), filters)
}

fn apply_column_values_filter(
    column: &crate::sql2::bind::expr::BoundColumnRef,
    values: &[BoundExpr],
    filters: &mut PlannedWriteFilters,
) -> Result<(), LixError> {
    let values = planned_values(values)?;
    if matches!(values, FilterSet::All) {
        return Ok(());
    }
    if let Some(existing) = filters
        .columns
        .iter_mut()
        .find(|filter| filter.column == *column)
    {
        existing.values.intersect_with(values);
    } else {
        filters.columns.push(PlannedColumnFilter {
            column: column.clone(),
            values,
        });
    }
    Ok(())
}

fn planned_values(values: &[BoundExpr]) -> Result<FilterSet<PlannedValue>, LixError> {
    literal_values(values, |value| match value {
        BoundLiteral::Null => Ok(None),
        BoundLiteral::Bool(value) => Ok(Some(PlannedValue::Bool(*value))),
        BoundLiteral::Integer(value) => Ok(Some(PlannedValue::Integer(*value))),
        BoundLiteral::Text(value) => Ok(Some(PlannedValue::Text(value.clone()))),
        BoundLiteral::Json(value) => serde_json::to_string(value)
            .map(PlannedValue::Json)
            .map(Some)
            .map_err(|error| {
                LixError::unknown(format!("failed to canonicalize JSON literal: {error}"))
            }),
        BoundLiteral::Blob(value) => Ok(Some(PlannedValue::Blob(value.clone()))),
    })
}

fn literal_values<T: Ord>(
    values: &[BoundExpr],
    mut convert: impl FnMut(&BoundLiteral) -> Result<Option<T>, LixError>,
) -> Result<FilterSet<T>, LixError> {
    let mut set = BTreeSet::new();
    for value in values {
        let BoundExpr::Literal(literal) = value else {
            return Ok(FilterSet::All);
        };
        let Some(value) = convert(literal)? else {
            continue;
        };
        set.insert(value);
    }

    if set.is_empty() {
        Ok(FilterSet::None)
    } else {
        Ok(FilterSet::Some(set))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql2::bind::{bind_statement, BoundStatement};
    use crate::sql2::parse_statement;
    use crate::sql2::plan::version_scope::VersionScope;

    #[test]
    fn plan_write_intersects_repeated_equality_predicates_to_none() {
        let plan = plan_sql(
            "UPDATE lix_state SET metadata = '{}' WHERE schema_key = 'profile' AND schema_key = 'note'",
        );

        assert_eq!(column_filter(&plan, "schema_key"), Some(FilterSet::None));
        assert_eq!(plan.filters.rows, FilterSet::All);
    }

    #[test]
    fn plan_write_intersects_repeated_in_predicates() {
        let plan = plan_sql(
            "DELETE FROM lix_state WHERE schema_key IN ('profile', 'note') AND schema_key IN ('note', 'task')",
        );

        assert_eq!(
            column_filter(&plan, "schema_key"),
            Some(FilterSet::Some(BTreeSet::from([PlannedValue::Text(
                "note".to_string()
            )])))
        );
    }

    #[test]
    fn plan_write_contradiction_does_not_drop_bound_params() {
        let plan = plan_sql(
            "UPDATE lix_state SET metadata = $1 WHERE schema_key IN ('profile') AND schema_key IN ('note') AND entity_id = $2",
        );

        assert_eq!(column_filter(&plan, "schema_key"), Some(FilterSet::None));
        assert_eq!(
            plan.bound.params.params.keys().copied().collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn plan_write_applies_active_version_scope_to_base_writes() {
        let plan = plan_sql("DELETE FROM lix_state WHERE schema_key = 'profile'");

        assert_eq!(
            plan.bound.version_scope,
            VersionScope::Active {
                version_id: "version1".to_string()
            }
        );
    }

    #[test]
    fn plan_write_keeps_explicit_required_scope_for_by_version_writes() {
        let plan = plan_sql("DELETE FROM lix_state_by_version WHERE version_id IN ('v1', 'v2')");

        assert_eq!(
            plan.bound.version_scope,
            VersionScope::ExplicitRequired {
                version_ids: BTreeSet::from(["v1".to_string(), "v2".to_string()])
            }
        );
        assert_eq!(
            column_filter(&plan, "version_id"),
            Some(FilterSet::Some(BTreeSet::from([
                PlannedValue::Text("v1".to_string()),
                PlannedValue::Text("v2".to_string())
            ])))
        );
    }

    #[test]
    fn plan_write_null_equality_filters_to_none() {
        let plan = plan_sql("DELETE FROM lix_state WHERE file_id = NULL");

        assert_eq!(column_filter(&plan, "file_id"), Some(FilterSet::None));
    }

    #[test]
    fn plan_write_false_conjunct_sets_no_match_sentinel() {
        let plan = plan_sql("UPDATE lix_file SET hidden = true WHERE id = 'file1' AND false");

        assert_eq!(plan.filters.rows, FilterSet::None);
        assert_eq!(column_filter(&plan, "id"), Some(FilterSet::None));
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
            column_filter(&plan, "schema_key"),
            Some(FilterSet::Some(BTreeSet::from([PlannedValue::Text(
                "draft".to_string()
            )])))
        );
        assert_eq!(
            plan.bound.version_scope,
            VersionScope::Active {
                version_id: "version1".to_string()
            }
        );
    }

    fn plan_sql(sql: &str) -> LogicalWritePlan {
        plan_sql_with_schemas(sql, &[])
    }

    fn plan_sql_with_schemas(sql: &str, schemas: &[serde_json::Value]) -> LogicalWritePlan {
        let statement = parse_statement(sql).expect("parse SQL");
        let bound = bind_statement(&statement, schemas, "version1").expect("bind SQL");
        let BoundStatement::Write(write) = bound else {
            panic!("expected bound write");
        };
        plan_write(write).expect("plan write")
    }

    fn column_filter(plan: &LogicalWritePlan, name: &str) -> Option<FilterSet<PlannedValue>> {
        plan.filters
            .columns
            .iter()
            .find(|filter| filter.column.name == name)
            .map(|filter| filter.values.clone())
    }
}
