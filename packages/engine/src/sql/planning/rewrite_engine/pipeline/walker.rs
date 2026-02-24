use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{Expr, ObjectNamePart, Query, TableFactor, Visit, Visitor};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryWalkSummary {
    pub(crate) relation_names: BTreeSet<String>,
    pub(crate) has_cte: bool,
    pub(crate) has_derived_tables: bool,
    pub(crate) has_expression_subqueries: bool,
}

pub(crate) fn walk_query(query: &Query) -> QueryWalkSummary {
    struct Collector {
        summary: QueryWalkSummary,
    }

    impl Visitor for Collector {
        type Break = ();

        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
            if query.with.is_some() {
                self.summary.has_cte = true;
            }
            ControlFlow::Continue(())
        }

        fn pre_visit_table_factor(
            &mut self,
            table_factor: &TableFactor,
        ) -> ControlFlow<Self::Break> {
            match table_factor {
                TableFactor::Table { name, .. } => {
                    if let Some(identifier) = name.0.last().and_then(ObjectNamePart::as_ident) {
                        self.summary
                            .relation_names
                            .insert(identifier.value.to_ascii_lowercase());
                    }
                }
                TableFactor::Derived { .. } => {
                    self.summary.has_derived_tables = true;
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            match expr {
                Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                    self.summary.has_expression_subqueries = true;
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector {
        summary: QueryWalkSummary {
            relation_names: BTreeSet::new(),
            has_cte: false,
            has_derived_tables: false,
            has_expression_subqueries: false,
        },
    };
    let _ = query.visit(&mut collector);
    collector.summary
}
