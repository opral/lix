use std::collections::{BTreeMap, BTreeSet};

use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::{Transformed, TreeNode};
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

pub(crate) fn canonicalize_json_identity_text_filters(
    schema: &Schema,
    filters: &[Expr],
) -> Result<Vec<Expr>, DataFusionError> {
    filters
        .iter()
        .cloned()
        .map(|filter| canonicalize_json_identity_text_filter(schema, filter))
        .collect()
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

pub(crate) fn json_predicate_placeholder_indexes_with_dfschema(
    schema: &DFSchema,
    expr: &Expr,
) -> BTreeSet<usize> {
    let mut indexes = BTreeSet::new();
    collect_json_predicate_placeholder_indexes(expr, &mut indexes, &|column| {
        schema
            .field_with_name(column.relation.as_ref(), &column.name)
            .ok()
            .map(|field| field.as_ref())
    });
    indexes
}

fn canonicalize_json_identity_text_filter(
    schema: &Schema,
    expr: Expr,
) -> Result<Expr, DataFusionError> {
    expr.transform(|expr| canonicalize_json_identity_text_expr(schema, expr))
        .map(|transformed| transformed.data)
}

fn canonicalize_json_identity_text_expr(
    schema: &Schema,
    expr: Expr,
) -> Result<Transformed<Expr>, DataFusionError> {
    match expr {
        Expr::BinaryExpr(binary) if is_comparison_operator(binary.op) => {
            canonicalize_json_identity_text_binary(schema, binary)
        }
        Expr::InList(in_list) => canonicalize_json_identity_text_in_list(schema, in_list),
        _ => Ok(Transformed::no(expr)),
    }
}

fn canonicalize_json_identity_text_binary(
    schema: &Schema,
    binary: BinaryExpr,
) -> Result<Transformed<Expr>, DataFusionError> {
    let BinaryExpr { left, op, right } = binary;
    let left_identity_json = is_identity_json_expr_for_arrow_schema(schema, &left);
    let right_identity_json = is_identity_json_expr_for_arrow_schema(schema, &right);
    let left = if right_identity_json {
        Box::new(canonicalize_json_text_literal(*left)?)
    } else {
        left
    };
    let right = if left_identity_json {
        Box::new(canonicalize_json_text_literal(*right)?)
    } else {
        right
    };
    Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
        left, op, right,
    ))))
}

fn canonicalize_json_identity_text_in_list(
    schema: &Schema,
    in_list: InList,
) -> Result<Transformed<Expr>, DataFusionError> {
    let expr_identity_json = is_identity_json_expr_for_arrow_schema(schema, &in_list.expr);
    let list_has_identity_json = in_list
        .list
        .iter()
        .any(|item| is_identity_json_expr_for_arrow_schema(schema, item));

    let expr = if list_has_identity_json {
        Box::new(canonicalize_json_text_literal(*in_list.expr)?)
    } else {
        in_list.expr
    };
    let list = if expr_identity_json {
        in_list
            .list
            .into_iter()
            .map(canonicalize_json_text_literal)
            .collect::<Result<Vec<_>, _>>()?
    } else {
        in_list.list
    };

    Ok(Transformed::yes(Expr::InList(InList::new(
        expr,
        list,
        in_list.negated,
    ))))
}

fn canonicalize_json_text_literal(expr: Expr) -> Result<Expr, DataFusionError> {
    let Expr::Literal(literal, metadata) = expr else {
        return Ok(expr);
    };
    let canonical = match &literal {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(canonical_json_text(value)?),
        _ => None,
    };
    Ok(match canonical {
        Some(value) => Expr::Literal(ScalarValue::Utf8(Some(value)), Some(json_field_metadata())),
        None => Expr::Literal(literal, metadata),
    })
}

fn canonical_json_text(raw: &str) -> Result<String, DataFusionError> {
    serde_json::from_str::<serde_json::Value>(raw)
        .map(|value| value.to_string())
        .map_err(|error| {
            lix_error_to_datafusion_error(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!("JSON comparison value is not valid JSON: {error}"),
            ))
        })
}

fn json_field_metadata() -> FieldMetadata {
    FieldMetadata::new(BTreeMap::from([(
        LIX_VALUE_TYPE_METADATA_KEY.to_string(),
        LIX_VALUE_TYPE_JSON.to_string(),
    )]))
}

fn is_identity_json_expr_for_arrow_schema(schema: &Schema, expr: &Expr) -> bool {
    is_identity_json_expr(expr)
        && is_json_expr(expr, &|column| {
            schema
                .fields()
                .iter()
                .find(|field| field.name() == &column.name)
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

fn collect_json_predicate_placeholder_indexes<'a>(
    expr: &'a Expr,
    indexes: &mut BTreeSet<usize>,
    lookup_field: &impl Fn(&datafusion::common::Column) -> Option<&'a Field>,
) {
    match expr {
        Expr::BinaryExpr(binary) if is_comparison_operator(binary.op) => {
            collect_json_predicate_placeholder_indexes(&binary.left, indexes, lookup_field);
            collect_json_predicate_placeholder_indexes(&binary.right, indexes, lookup_field);
            if is_json_expr(&binary.left, lookup_field) {
                collect_placeholder_indexes(&binary.right, indexes);
            }
            if is_json_expr(&binary.right, lookup_field) {
                collect_placeholder_indexes(&binary.left, indexes);
            }
        }
        Expr::BinaryExpr(binary) => {
            collect_json_predicate_placeholder_indexes(&binary.left, indexes, lookup_field);
            collect_json_predicate_placeholder_indexes(&binary.right, indexes, lookup_field);
        }
        Expr::InList(in_list) => {
            collect_json_predicate_placeholder_indexes(&in_list.expr, indexes, lookup_field);
            for item in &in_list.list {
                collect_json_predicate_placeholder_indexes(item, indexes, lookup_field);
            }
            if is_json_expr(&in_list.expr, lookup_field) {
                for item in &in_list.list {
                    collect_placeholder_indexes(item, indexes);
                }
            }
            for item in &in_list.list {
                if is_json_expr(item, lookup_field) {
                    collect_placeholder_indexes(&in_list.expr, indexes);
                }
            }
        }
        Expr::Between(between) => {
            collect_json_predicate_placeholder_indexes(&between.expr, indexes, lookup_field);
            collect_json_predicate_placeholder_indexes(&between.low, indexes, lookup_field);
            collect_json_predicate_placeholder_indexes(&between.high, indexes, lookup_field);
            if is_json_expr(&between.expr, lookup_field) {
                collect_placeholder_indexes(&between.low, indexes);
                collect_placeholder_indexes(&between.high, indexes);
            }
        }
        Expr::Alias(alias) => {
            collect_json_predicate_placeholder_indexes(&alias.expr, indexes, lookup_field)
        }
        Expr::Not(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsNotUnknown(inner)
        | Expr::Negative(inner) => {
            collect_json_predicate_placeholder_indexes(inner, indexes, lookup_field)
        }
        Expr::Cast(cast) => {
            collect_json_predicate_placeholder_indexes(&cast.expr, indexes, lookup_field)
        }
        Expr::TryCast(cast) => {
            collect_json_predicate_placeholder_indexes(&cast.expr, indexes, lookup_field)
        }
        Expr::ScalarFunction(function) => {
            for arg in &function.args {
                collect_json_predicate_placeholder_indexes(arg, indexes, lookup_field);
            }
        }
        Expr::Case(case) => {
            if let Some(expr) = &case.expr {
                collect_json_predicate_placeholder_indexes(expr, indexes, lookup_field);
            }
            for (when, then) in &case.when_then_expr {
                collect_json_predicate_placeholder_indexes(when, indexes, lookup_field);
                collect_json_predicate_placeholder_indexes(then, indexes, lookup_field);
            }
            if let Some(expr) = &case.else_expr {
                collect_json_predicate_placeholder_indexes(expr, indexes, lookup_field);
            }
        }
        _ => {}
    }
}

fn collect_placeholder_indexes(expr: &Expr, indexes: &mut BTreeSet<usize>) {
    if let Expr::Placeholder(placeholder) = expr {
        if let Some(index) = placeholder
            .id
            .strip_prefix('$')
            .and_then(|value| value.parse::<usize>().ok())
        {
            indexes.insert(index);
        }
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

fn is_identity_json_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Column(column) => matches!(column.name.as_str(), "entity_pk" | "lixcol_entity_pk"),
        Expr::Alias(alias) => is_identity_json_expr(&alias.expr),
        Expr::Cast(cast) => is_identity_json_expr(&cast.expr),
        Expr::TryCast(cast) => is_identity_json_expr(&cast.expr),
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
