use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{DFSchema, DataFusionError, ScalarValue};
use datafusion::logical_expr::expr::{Between, InList};
use datafusion::logical_expr::{BinaryExpr, Expr, Like, Operator};

use crate::LixError;

use super::error::lix_error_to_datafusion_error;
use super::result_metadata::{field_is_json, LIX_VALUE_TYPE_JSON, LIX_VALUE_TYPE_METADATA_KEY};

pub(crate) fn validate_json_predicate_filters(
    schema: &Schema,
    filters: &[Expr],
) -> Result<(), DataFusionError> {
    for filter in filters {
        validate_json_predicate_expr_with_arrow_schema(schema, filter)
            .map_err(lix_error_to_datafusion_error)?;
    }
    Ok(())
}

pub(crate) fn validate_json_predicate_expr_with_dfschema(
    schema: &DFSchema,
    expr: &Expr,
) -> Result<(), LixError> {
    validate_expr(expr, &|column| {
        schema
            .field_with_name(column.relation.as_ref(), &column.name)
            .ok()
            .map(|field| field.as_ref())
    })
}

fn validate_json_predicate_expr_with_arrow_schema(
    schema: &Schema,
    expr: &Expr,
) -> Result<(), LixError> {
    validate_expr(expr, &|column| {
        schema
            .fields()
            .iter()
            .find(|field| field.name() == &column.name)
            .map(|field| field.as_ref())
    })
}

fn validate_expr<'a>(
    expr: &'a Expr,
    lookup_field: &impl Fn(&datafusion::common::Column) -> Option<&'a Field>,
) -> Result<(), LixError> {
    match expr {
        Expr::BinaryExpr(binary) => validate_binary_expr(binary, lookup_field),
        Expr::InList(in_list) => validate_in_list(in_list, lookup_field),
        Expr::Between(between) => validate_between(between, lookup_field),
        Expr::Like(like) | Expr::SimilarTo(like) => validate_like(like, lookup_field),
        Expr::Alias(alias) => validate_expr(&alias.expr, lookup_field),
        Expr::Not(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsNotUnknown(inner)
        | Expr::Negative(inner) => validate_expr(inner, lookup_field),
        Expr::Cast(cast) => validate_expr(&cast.expr, lookup_field),
        Expr::TryCast(cast) => validate_expr(&cast.expr, lookup_field),
        Expr::ScalarFunction(function) => {
            for arg in &function.args {
                validate_expr(arg, lookup_field)?;
            }
            Ok(())
        }
        Expr::Case(case) => {
            if let Some(expr) = &case.expr {
                validate_expr(expr, lookup_field)?;
            }
            for (when, then) in &case.when_then_expr {
                validate_expr(when, lookup_field)?;
                validate_expr(then, lookup_field)?;
            }
            if let Some(expr) = &case.else_expr {
                validate_expr(expr, lookup_field)?;
            }
            Ok(())
        }
        Expr::AggregateFunction(function) => {
            for arg in &function.params.args {
                validate_expr(arg, lookup_field)?;
            }
            Ok(())
        }
        Expr::WindowFunction(function) => {
            for arg in &function.params.args {
                validate_expr(arg, lookup_field)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_binary_expr<'a>(
    binary: &'a BinaryExpr,
    lookup_field: &impl Fn(&datafusion::common::Column) -> Option<&'a Field>,
) -> Result<(), LixError> {
    validate_expr(&binary.left, lookup_field)?;
    validate_expr(&binary.right, lookup_field)?;

    if !is_comparison_operator(binary.op) {
        return Ok(());
    }

    validate_comparison_operands(&binary.left, &binary.right, lookup_field)
}

fn validate_in_list<'a>(
    in_list: &'a InList,
    lookup_field: &impl Fn(&datafusion::common::Column) -> Option<&'a Field>,
) -> Result<(), LixError> {
    validate_expr(&in_list.expr, lookup_field)?;
    for item in &in_list.list {
        validate_expr(item, lookup_field)?;
    }

    if is_json_expr(&in_list.expr, lookup_field) {
        for item in &in_list.list {
            require_json_comparison_operand(item, lookup_field)?;
        }
    }

    for item in &in_list.list {
        if is_json_expr(item, lookup_field) {
            require_json_comparison_operand(&in_list.expr, lookup_field)?;
        }
    }

    Ok(())
}

fn validate_between<'a>(
    between: &'a Between,
    lookup_field: &impl Fn(&datafusion::common::Column) -> Option<&'a Field>,
) -> Result<(), LixError> {
    validate_expr(&between.expr, lookup_field)?;
    validate_expr(&between.low, lookup_field)?;
    validate_expr(&between.high, lookup_field)?;

    if is_json_expr(&between.expr, lookup_field) {
        require_json_comparison_operand(&between.low, lookup_field)?;
        require_json_comparison_operand(&between.high, lookup_field)?;
    }

    Ok(())
}

fn validate_like<'a>(
    like: &'a Like,
    lookup_field: &impl Fn(&datafusion::common::Column) -> Option<&'a Field>,
) -> Result<(), LixError> {
    validate_expr(&like.expr, lookup_field)?;
    validate_expr(&like.pattern, lookup_field)?;

    if is_json_expr(&like.expr, lookup_field) {
        return Err(json_predicate_type_error(&like.expr));
    }

    Ok(())
}

fn validate_comparison_operands<'a>(
    left: &'a Expr,
    right: &'a Expr,
    lookup_field: &impl Fn(&datafusion::common::Column) -> Option<&'a Field>,
) -> Result<(), LixError> {
    let left_is_json = is_json_expr(left, lookup_field);
    let right_is_json = is_json_expr(right, lookup_field);

    if left_is_json {
        require_json_comparison_operand(right, lookup_field)?;
    }
    if right_is_json {
        require_json_comparison_operand(left, lookup_field)?;
    }

    Ok(())
}

fn require_json_comparison_operand<'a>(
    expr: &'a Expr,
    lookup_field: &impl Fn(&datafusion::common::Column) -> Option<&'a Field>,
) -> Result<(), LixError> {
    if is_json_expr(expr, lookup_field)
        || is_null_literal(expr)
        || matches!(expr, Expr::Placeholder(_))
    {
        return Ok(());
    }

    Err(json_predicate_type_error(expr))
}

fn is_json_expr<'a>(
    expr: &'a Expr,
    lookup_field: &impl Fn(&datafusion::common::Column) -> Option<&'a Field>,
) -> bool {
    match expr {
        Expr::Column(column) => lookup_field(column).is_some_and(field_is_json),
        Expr::Literal(_, Some(metadata)) => metadata
            .inner()
            .get(LIX_VALUE_TYPE_METADATA_KEY)
            .is_some_and(|value| value == LIX_VALUE_TYPE_JSON),
        Expr::ScalarFunction(function) => matches!(function.name(), "lix_json" | "lix_json_get"),
        Expr::Alias(alias) => is_json_expr(&alias.expr, lookup_field),
        Expr::Cast(cast) => is_json_expr(&cast.expr, lookup_field),
        Expr::TryCast(cast) => is_json_expr(&cast.expr, lookup_field),
        _ => false,
    }
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(value, _) if matches!(value, ScalarValue::Null))
}

fn is_comparison_operator(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}

fn json_predicate_type_error(expr: &Expr) -> LixError {
    LixError::new(
        LixError::CODE_TYPE_MISMATCH,
        format!("JSON columns can only be compared with JSON expressions, got {expr}"),
    )
    .with_hint("Wrap JSON text with lix_json(...), use lix_json_get(...) for JSON values, or use IS NULL for null checks.")
}
