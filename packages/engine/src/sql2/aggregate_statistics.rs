//! Completes DataFusion's aggregate-statistics optimization for exact SUM/AVG.
//!
//! DataFusion already removes ungrouped COUNT/MIN/MAX scans when a source
//! exposes exact statistics. Its built-in rule currently leaves SUM and AVG
//! unresolved, which prevents the all-or-nothing rewrite. This rule uses the
//! same physical-plan contract and adds those two standard aggregates. It is
//! provider-agnostic and never inspects SQL text.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::functions_aggregate::{average::Avg, sum::Sum};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateInputMode};
use datafusion::physical_plan::expressions;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::udaf::{AggregateFunctionExpr, StatisticsArgs};
use datafusion::physical_plan::{ExecutionPlan, Statistics};

#[derive(Debug, Default)]
pub(crate) struct ExactAggregateStatistics;

impl PhysicalOptimizerRule for ExactAggregateStatistics {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(partial) = optimizable_partial_aggregate(plan.as_ref()) {
            let aggregate = partial
                .as_any()
                .downcast_ref::<AggregateExec>()
                .expect("optimizable partial aggregate is AggregateExec");
            let statistics = aggregate.input().partition_statistics(None)?;
            let mut projections = Vec::with_capacity(aggregate.aggr_expr().len());
            for expression in aggregate.aggr_expr() {
                let Some(value) = exact_aggregate_value(&statistics, expression) else {
                    return plan
                        .map_children(|child| self.optimize(child, config).map(Transformed::yes))
                        .data();
                };
                projections.push(ProjectionExpr {
                    expr: expressions::lit(value),
                    alias: expression.name().to_string(),
                });
            }
            return Ok(Arc::new(ProjectionExec::try_new(
                projections,
                Arc::new(PlaceholderRowExec::new(plan.schema())),
            )?));
        }
        plan.map_children(|child| self.optimize(child, config).map(Transformed::yes))
            .data()
    }

    fn name(&self) -> &str {
        "lix_exact_aggregate_statistics"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

fn optimizable_partial_aggregate(plan: &dyn ExecutionPlan) -> Option<Arc<dyn ExecutionPlan>> {
    let final_aggregate = plan.as_any().downcast_ref::<AggregateExec>()?;
    if final_aggregate.mode().input_mode() != AggregateInputMode::Partial
        || !final_aggregate.group_expr().is_empty()
    {
        return None;
    }
    let mut child = Arc::clone(final_aggregate.input());
    loop {
        if let Some(partial) = child.as_any().downcast_ref::<AggregateExec>()
            && partial.mode().input_mode() == AggregateInputMode::Raw
            && partial.group_expr().is_empty()
            && partial.filter_expr().iter().all(Option::is_none)
            // An operator tree that changes rows can expose exact-looking
            // column statistics that do not prove the aggregate over its
            // output. Restrict this shortcut to a leaf source whose
            // statistics apply directly to every consumed row.
            && partial.input().children().is_empty()
        {
            return Some(child);
        }
        let children = child.children();
        let [next] = children.as_slice() else {
            return None;
        };
        child = Arc::clone(next);
    }
}

fn exact_aggregate_value(
    statistics: &Statistics,
    expression: &AggregateFunctionExpr,
) -> Option<ScalarValue> {
    let field = expression.field();
    let arguments = expression.expressions();
    let statistics_args = StatisticsArgs {
        statistics,
        return_type: field.data_type(),
        is_distinct: expression.is_distinct(),
        exprs: &arguments,
    };
    if let Some(value) = expression.fun().value_from_stats(&statistics_args) {
        return Some(value);
    }
    if expression.is_distinct() || arguments.len() != 1 {
        return None;
    }
    let column = arguments[0].as_any().downcast_ref::<Column>()?;
    let column_statistics = statistics.column_statistics.get(column.index())?;
    if expression.fun().inner().as_any().is::<Sum>() {
        exact_scalar(&column_statistics.sum_value)
            .and_then(|value| value.cast_to(field.data_type()).ok())
    } else if expression.fun().inner().as_any().is::<Avg>() {
        let row_count = *exact_usize(&statistics.num_rows)?;
        let null_count = *exact_usize(&column_statistics.null_count)?;
        let count = row_count.checked_sub(null_count)?;
        if count == 0 {
            return ScalarValue::try_new_null(field.data_type()).ok();
        }
        let sum = exact_scalar(&column_statistics.sum_value)?;
        let sum = match sum {
            ScalarValue::Int64(Some(value)) => value as f64,
            ScalarValue::Float64(Some(value)) => value,
            _ => return None,
        };
        ScalarValue::Float64(Some(sum / count as f64))
            .cast_to(field.data_type())
            .ok()
    } else {
        None
    }
}

fn exact_scalar(value: &Precision<ScalarValue>) -> Option<ScalarValue> {
    match value {
        Precision::Exact(value) => Some(value.clone()),
        Precision::Inexact(_) | Precision::Absent => None,
    }
}

fn exact_usize(value: &Precision<usize>) -> Option<&usize> {
    match value {
        Precision::Exact(value) => Some(value),
        Precision::Inexact(_) | Precision::Absent => None,
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::stats::ColumnStatistics;
    use datafusion::functions_aggregate::{average::avg_udaf, sum::sum_udaf};
    use datafusion::logical_expr::function::AccumulatorArgs;
    use datafusion::logical_expr::{
        Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
    };
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;

    use super::*;

    fn aggregate_expr(
        function: Arc<AggregateUDF>,
        input_type: DataType,
        distinct: bool,
    ) -> AggregateFunctionExpr {
        let schema = Arc::new(Schema::new(vec![Field::new("value", input_type, true)]));
        AggregateExprBuilder::new(function, vec![Arc::new(Column::new("value", 0))])
            .schema(schema)
            .alias("result")
            .with_distinct(distinct)
            .build()
            .unwrap()
    }

    fn statistics(
        rows: Precision<usize>,
        nulls: Precision<usize>,
        sum: Precision<ScalarValue>,
    ) -> Statistics {
        Statistics {
            num_rows: rows,
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics::new_unknown()
                    .with_null_count(nulls)
                    .with_sum_value(sum),
            ],
        }
    }

    #[test]
    fn rewrites_builtin_sum_from_exact_statistics() {
        let expression = aggregate_expr(sum_udaf(), DataType::Int64, false);
        let statistics = statistics(
            Precision::Exact(4),
            Precision::Exact(1),
            Precision::Exact(ScalarValue::Int64(Some(9))),
        );

        assert_eq!(
            exact_aggregate_value(&statistics, &expression),
            Some(ScalarValue::Int64(Some(9)))
        );
    }

    #[test]
    fn rewrites_builtin_avg_from_exact_statistics() {
        let expression = aggregate_expr(avg_udaf(), DataType::Float64, false);
        let statistics = statistics(
            Precision::Exact(4),
            Precision::Exact(1),
            Precision::Exact(ScalarValue::Float64(Some(9.0))),
        );

        assert_eq!(
            exact_aggregate_value(&statistics, &expression),
            Some(ScalarValue::Float64(Some(3.0)))
        );
    }

    #[test]
    fn does_not_rewrite_distinct_or_inexact_sum() {
        let exact_statistics = statistics(
            Precision::Exact(4),
            Precision::Exact(0),
            Precision::Exact(ScalarValue::Int64(Some(9))),
        );
        let distinct = aggregate_expr(sum_udaf(), DataType::Int64, true);
        assert_eq!(exact_aggregate_value(&exact_statistics, &distinct), None);

        let inexact_statistics = statistics(
            Precision::Exact(4),
            Precision::Exact(0),
            Precision::Inexact(ScalarValue::Int64(Some(9))),
        );
        let sum = aggregate_expr(sum_udaf(), DataType::Int64, false);
        assert_eq!(exact_aggregate_value(&inexact_statistics, &sum), None);
    }

    #[test]
    fn handles_all_null_input_conservatively() {
        let statistics = statistics(Precision::Exact(4), Precision::Exact(4), Precision::Absent);
        let sum = aggregate_expr(sum_udaf(), DataType::Int64, false);
        assert_eq!(exact_aggregate_value(&statistics, &sum), None);

        let avg = aggregate_expr(avg_udaf(), DataType::Float64, false);
        assert_eq!(
            exact_aggregate_value(&statistics, &avg),
            Some(ScalarValue::Float64(None))
        );
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct CustomNamedSum {
        signature: Signature,
    }

    impl CustomNamedSum {
        fn new() -> Self {
            Self {
                signature: Signature::uniform(1, vec![DataType::Int64], Volatility::Immutable),
            }
        }
    }

    impl AggregateUDFImpl for CustomNamedSum {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "sum"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int64)
        }

        fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
            unreachable!("the test only builds the physical expression")
        }
    }

    #[test]
    fn does_not_rewrite_custom_aggregate_with_builtin_name() {
        let custom_sum = Arc::new(AggregateUDF::from(CustomNamedSum::new()));
        let expression = aggregate_expr(custom_sum, DataType::Int64, false);
        let statistics = statistics(
            Precision::Exact(4),
            Precision::Exact(0),
            Precision::Exact(ScalarValue::Int64(Some(9))),
        );

        assert_eq!(exact_aggregate_value(&statistics, &expression), None);
    }
}
